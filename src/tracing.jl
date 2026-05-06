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
    log_channel::Union{RemoteChannel, Nothing}
    drain_task::Union{Task, Nothing}
    worker_ios::Dict{Int, IOStream}
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

    # Remote channel for worker log records
    log_channel = RemoteChannel(() -> Channel{Dict{String,Any}}(256))
    worker_ios = Dict{Int, IOStream}()

    # Drain task: reads worker records from the channel and writes to all.jsonl + per-worker files
    drain_task = @async begin
        try
            for record in log_channel
                line = sprint(JSON.print, record) * "\n"
                write(all_io, line)
                flush(all_io)
                # Write to per-worker file
                pid = get(record, "worker_pid", nothing)
                if pid !== nothing
                    pid = Int(pid)
                    if !haskey(worker_ios, pid)
                        worker_ios[pid] = open(worker_trace_path(config, pid), "a")
                    end
                    write(worker_ios[pid], line)
                    flush(worker_ios[pid])
                end
                # Also route errors to errors.jsonl
                lvl = get(record, "level", "")
                if lvl in ("Warn", "Error")
                    write(errors_io, line)
                    flush(errors_io)
                end
            end
        catch e
            e isa InvalidStateException || @debug "log drain error" exception=(e, catch_backtrace())
        end
    end

    @info "tracing started" run_id=config.run_id output_dir=dir

    TracingState(config, previous_logger, all_io, errors_io, log_channel, drain_task, worker_ios)
end

"""
    teardown_tracing(state::TracingState)

Restore the previous global logger and close trace file handles.
"""
function teardown_tracing(state::TracingState)
    @info "tracing stopped" run_id=state.config.run_id

    Logging.global_logger(state.previous_logger)

    # Close the remote channel so the drain task finishes
    if state.log_channel !== nothing
        try
            close(state.log_channel)
        catch
        end
    end
    # Wait for drain to finish writing
    if state.drain_task !== nothing
        try
            wait(state.drain_task)
        catch
        end
    end
    # Close per-worker IO handles
    for (_, io) in state.worker_ios
        try
            close(io)
        catch
        end
    end

    state.all_io !== nothing && close(state.all_io)
    state.errors_io !== nothing && close(state.errors_io)
    nothing
end

# ── Worker-side logger setup ─────────────────────────────────────────────────

"""
    setup_worker_tracing(log_channel::RemoteChannel, pid::Int)

To be called via `remotecall` on a worker process.  Installs a custom logger
that sends structured JSONL records back to the coordinator via `log_channel`.
"""
function setup_worker_tracing(log_channel::RemoteChannel, pid::Int)
    hostname = gethostname()
    worker_sink = FormatLogger() do io, log_args
        record = Dict{String, Any}(
            "timestamp" => Dates.format(Dates.now(Dates.UTC), "yyyy-mm-ddTHH:MM:SS.sssZ"),
            "level"     => string(log_args.level),
            "module"    => string(log_args._module),
            "message"   => string(log_args.message),
            "file"      => string(log_args.file),
            "line"      => log_args.line,
            "worker_pid" => pid,
            "hostname"  => hostname,
        )
        for (k, v) in log_args.kwargs
            record[string(k)] = _safe_serialize(v)
        end
        try
            put!(log_channel, record)
        catch
            # Channel closed or coordinator gone — silently drop
        end
    end
    existing = Logging.global_logger()
    logger = TeeLogger(existing, worker_sink)
    Logging.global_logger(logger)
    nothing
end
