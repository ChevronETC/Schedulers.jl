# ── tracing.jl ───────────────────────────────────────────────────────────────
#
# Structured logging and tracing for Schedulers.jl using LoggingExtras.
#
# Sets up a composable logger tree that routes all log messages from
# Schedulers, manager backends, and user code into structured JSONL files
# while preserving normal console output.
#
# Logger tree:

# ── Configuration ─────────────────────────────────────────────────────────────

"""
    TracingConfig(; output_dir, run_id, console_level)

Configuration for structured tracing.  Pass as `tracing` kwarg to `SchedulerOptions`.

# Fields
- `output_dir::String` — root directory for trace output (default: `/tmp/schedulers`)
- `run_id::String` — unique run identifier (default: generated)
- `console_level::LogLevel` — minimum level for console output (default: `Info`)
"""
struct TracingConfig
    output_dir::String
    run_id::String
    console_level::Logging.LogLevel
end

function TracingConfig(;
        output_dir::String = "/tmp/schedulers",
        run_id::String = "",
        console_level::Logging.LogLevel = Logging.Info)
    if isempty(run_id)
        run_id = string(Dates.format(Dates.now(), "yyyymmdd-HHMMSS"), "-", randstring(6))
    end
    TracingConfig(output_dir, run_id, console_level)
end

"""Return the full path to this run's trace directory."""
run_dir(config::TracingConfig) = joinpath(config.output_dir, config.run_id)

"""Return the path to a worker's trace file."""
worker_trace_path(config::TracingConfig, pid::Int) = joinpath(run_dir(config), "workers", "worker-$pid.jsonl")

# ── JSONL formatter ───────────────────────────────────────────────────────────

# Task-local context: set by the work loop so the formatter can tag records
const _task_context = Dict{UInt, Dict{Symbol, Any}}()
const _task_context_lock = ReentrantLock()

"""
    set_task_context!(; kwargs...)

Set task-local context fields (e.g. `task_id`, `worker_pid`, `hostname`) that
will be injected into every JSONL log record emitted from the current task.
"""
function set_task_context!(; kwargs...)
    tid = objectid(current_task())
    lock(_task_context_lock) do
        ctx = get!(() -> Dict{Symbol, Any}(), _task_context, tid)
        for (k, v) in kwargs
            ctx[k] = v
        end
    end
end

"""
    clear_task_context!()

Remove all task-local context for the current task.
"""
function clear_task_context!()
    tid = objectid(current_task())
    lock(_task_context_lock) do
        delete!(_task_context, tid)
    end
end

function _get_task_context()
    tid = objectid(current_task())
    lock(_task_context_lock) do
        get(_task_context, tid, Dict{Symbol, Any}())
    end
end

"""
    _jsonl_format(io, log_args)

Write a single JSONL line with timestamp, level, module, message, kwargs,
and injected task context.
"""
function _jsonl_format(io, log_args)
    ctx = _get_task_context()
    record = Dict{String, Any}(
        "timestamp" => Dates.format(Dates.now(Dates.UTC), "yyyy-mm-ddTHH:MM:SS.sssZ"),
        "level"     => string(log_args.level),
        "module"    => string(log_args._module),
        "message"   => string(log_args.message),
        "file"      => string(log_args.file),
        "line"      => log_args.line,
    )
    # Inject task context
    for (k, v) in ctx
        record[string(k)] = v
    end
    # Inject structured kwargs from the log call
    for (k, v) in log_args.kwargs
        record[string(k)] = _safe_serialize(v)
    end
    JSON.print(io, record)
    println(io)
end

_safe_serialize(x::Number) = x
_safe_serialize(x::AbstractString) = x
_safe_serialize(x::Symbol) = string(x)
_safe_serialize(x::Bool) = x
_safe_serialize(x::Nothing) = nothing
_safe_serialize(x::Exception) = Dict("type" => string(typeof(x)), "message" => sprint(showerror, x))
_safe_serialize(x::AbstractVector) = [_safe_serialize(v) for v in x]
_safe_serialize(x::AbstractDict) = Dict(string(k) => _safe_serialize(v) for (k, v) in x)
_safe_serialize(x::NamedTuple) = Dict(string(k) => _safe_serialize(v) for (k, v) in pairs(x))
_safe_serialize(x::Tuple) = [_safe_serialize(v) for v in x]
_safe_serialize(x) = string(x)

# ── Tracing state ─────────────────────────────────────────────────────────────

mutable struct TracingState
    config::TracingConfig
    previous_logger::Any
    all_io::Union{IOStream, Nothing}
    errors_io::Union{IOStream, Nothing}
end

# ── Setup / Teardown ──────────────────────────────────────────────────────────

"""
    setup_tracing(config::TracingConfig) -> TracingState

Create the trace directory structure, build the logger tree, and install it
as the global logger.  Returns a `TracingState` that must be passed to
`teardown_tracing` to restore the previous logger.
"""
function setup_tracing(config::TracingConfig)::TracingState
    dir = run_dir(config)
    mkpath(dir)
    mkpath(joinpath(dir, "workers"))

    all_io = open(joinpath(dir, "all.jsonl"), "a")
    errors_io = open(joinpath(dir, "errors.jsonl"), "a")

    all_sink = FormatLogger(_jsonl_format, all_io)
    errors_sink = FormatLogger(_jsonl_format, errors_io)

    logger = TeeLogger(
        MinLevelLogger(Logging.ConsoleLogger(stderr), config.console_level),
        all_sink,
        MinLevelLogger(errors_sink, Logging.Warn),
    )

    previous_logger = Logging.global_logger(logger)

    @info "tracing started" run_id=config.run_id output_dir=dir

    TracingState(config, previous_logger, all_io, errors_io)
end

"""
    teardown_tracing(state::TracingState)

Restore the previous global logger and close trace file handles.
"""
function teardown_tracing(state::TracingState)
    @info "tracing stopped" run_id=state.config.run_id

    Logging.global_logger(state.previous_logger)

    state.all_io !== nothing && close(state.all_io)
    state.errors_io !== nothing && close(state.errors_io)
    nothing
end

# ── Worker-side logger setup ─────────────────────────────────────────────────

"""
    setup_worker_tracing(config::TracingConfig, pid::Int)

To be called via `remotecall` on a worker process.  Installs a FormatLogger
that writes structured JSONL to the worker's trace file while keeping
the existing logger as a tee target.
"""
function setup_worker_tracing(config::TracingConfig, pid::Int)
    path = worker_trace_path(config, pid)
    mkpath(dirname(path))
    io = open(path, "a")
    worker_sink = FormatLogger(_jsonl_format, io)
    existing = Logging.global_logger()
    logger = TeeLogger(existing, worker_sink)
    Logging.global_logger(logger)
    nothing
end
