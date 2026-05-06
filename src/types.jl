# --- Exception types ---

struct TimeoutException <: Exception
    pid::Int
    elapsed::Float64
end

struct PreemptException <: Exception end

struct WorkerLostException <: Exception
    pid::Int
    hostname::String
end
WorkerLostException(pid::Int) = WorkerLostException(pid, "")

struct TooManyErrorsException <: Exception
    nerrors::Int
    maxerrors::Int
end

struct InitializationException <: Exception
    pid::Int
    phase::String  # "hostname", "modules", "functions", "init"
    cause::Exception
end

struct CheckpointException <: Exception
    pid::Int
    checkpoint::Any
    operation::String  # "save", "load", "remove"
    cause::Exception
end

struct ReductionException <: Exception
    cause::Exception
end

struct ScalingException <: Exception
    operation::String  # "addprocs", "rmprocs"
    cause::Exception
end

# --- ExceptionAction ---

struct ExceptionAction
    bad_pid::Bool
    do_break::Bool
    do_interrupt::Bool
    do_error::Bool
    retry_task::Bool  # true = worker fault (retry on another worker), false = task fault (don't retry)
end

# --- ElasticLoop ---

mutable struct ElasticLoop{FAddProcs<:Function,FInit<:Function,FMinWorkers<:Function,FMaxWorkers<:Function,FNWorkers<:Function,FTrigger<:Function,FSave<:Function,FQuantum<:Function,T,C}
    epmap_use_master::Bool
    initialized_pids::Set{Int}
    used_pids_map::Set{Int}
    used_pids_reduce::Set{Int}
    pid_channel_map_add::Channel{Int}
    pid_channel_reduce_add::Channel{Int}
    reduce_trigger_channel::Channel{Bool}
    epmap_addprocs::FAddProcs
    epmap_init::FInit
    is_reduce_triggered::Bool
    epmap_reduce_trigger::FTrigger
    epmap_save_partial_reduction::FSave
    epmap_minworkers::FMinWorkers
    epmap_maxworkers::FMaxWorkers
    epmap_quantum::FQuantum
    epmap_nworkers::FNWorkers
    tsk_pool_todo::Vector{T}
    tsk_pool_done::Vector{T}
    tsk_pool_timed_out::Vector{T}
    tsk_pool_reduced::Vector{T}
    tsk_count::Int
    reduce_checkpoints::Vector{C}
    reduce_checkpoints_snapshot::Vector{C}
    checkpoints_are_flushed::Bool
    reduce_checkpoints_is_dirty::Dict{Int,Bool}
    checkpoints::Dict{Int,Union{C,Nothing}}
    interrupted::Bool
    errored::Bool
    pid_failures::Dict{Int,Int}
    tsk_retried::Set{Int}
    grace_period_start_time::Float64
    null_tsk_runtime_threshold::Float64
    skip_tsk_tol_ratio::Float64
    grace_period_ratio::Float64
    state_lock::ReentrantLock
    events::Channel{SchedulerEvent}
end
function ElasticLoop(::Type{C}, tasks, options; isreduce) where {C}
    _tsk_pool_todo = vec(collect(tasks))

    eloop = ElasticLoop(
        options.usemaster,
        options.usemaster ? Set{Int}() : Set(1),
        options.usemaster ? Set{Int}() : Set(1),
        Set{Int}(),
        Channel{Int}(Inf),
        Channel{Int}(Inf),
        Channel{Bool}(1),
        options.addprocs,
        options.init,
        false,
        options.reduce_trigger,
        options.save_partial_reduction,
        options.minworkers,
        options.maxworkers,
        options.quantum,
        options.nworkers,
        _tsk_pool_todo,
        empty(_tsk_pool_todo),
        empty(_tsk_pool_todo),
        empty(_tsk_pool_todo),
        length(_tsk_pool_todo),
        C[],
        C[],
        false,
        Dict{Int,Bool}(),
        Dict{Int,Union{Nothing,C}}(),
        false,
        false,
        Dict{Int,Int}(),
        Set{Int}(),
        Inf,
        options.null_tsk_runtime_threshold,
        options.skip_tsk_tol_ratio,
        options.grace_period_ratio,
        ReentrantLock(),
        Channel{SchedulerEvent}(Inf),
    )

    if !isreduce
        close(eloop.pid_channel_reduce_add)
    end

    eloop
end

# --- Utility functions on ElasticLoop ---

"""
    n = total_tasks(eloop)

Given `eloop::ElasticLoop`, return the number of total tasks that are being mapped over.
"""
total_tasks(eloop::ElasticLoop) = eloop.tsk_count

"""
    tsks = pending_tasks(eloop)

Given `eloop::ElasticLoop`, return a list of tasks that are still pending.
"""
pending_tasks(eloop::ElasticLoop) = eloop.tsk_pool_todo

"""
    tsks = complete_tasks(eloop)

Given `eloop::ElasticLoop`, return a list of tasks that are complete.
"""
complete_tasks(eloop::ElasticLoop) = eloop.tsk_pool_done

"""
    tsks = reduced_tasks(eloop)

Given `eloop::ElasticLoop`, return a list of tasks that are complete and reduced.
"""
reduced_tasks(eloop::ElasticLoop) = eloop.tsk_pool_reduced

# for performance metrics, track when the pid is started
const _pid_up_timestamp = Dict{Int, Float64}()

# --- SchedulerOptions ---

default_reducer!(x, y) = (x .+= y; nothing)

mutable struct SchedulerOptions{C}
    retries::Int
    maxerrors::Int
    timeout_multiplier::Float64
    timeout_function_multiplier::Float64
    null_tsk_runtime_threshold::Float64
    skip_tsk_tol_ratio::Float64
    grace_period_ratio::Float64
    skip_tasks_that_timeout::Bool
    minworkers::Function
    maxworkers::Function
    nworkers::Function
    usemaster::Bool
    quantum::Function
    addprocs::Function
    init::Function
    preempt_channel_future::Function
    checkpoint_task::Function
    restart_task::Function
    reporttasks::Bool
    keepcheckpoints::Bool
    journalfile::String
    journal_init_callback::Function
    journal_task_callback::Function
    # reduce specific:
    reducer!::Function
    zeros::Function
    scratch::Vector{C}
    id::String
    epmapreduce_fetch::Function
    save_checkpoint::Function
    load_checkpoint::Function
    rm_checkpoint::Function
    reduce_trigger::Function
    save_partial_reduction::Function
    gethostname::Function
    # manager backend integration:
    manager_event_forwarder::Function  # (Channel{SchedulerEvent}) -> cleanup_fn or nothing
    manager_metrics::Function          # () -> NamedTuple
    # tracing:
    tracing::Union{TracingConfig, Nothing}
end

function SchedulerOptions(;
        retries = 0,
        maxerrors = typemax(Int),
        timeout_multiplier = 5,
        timeout_function_multiplier = 5,
        null_tsk_runtime_threshold = 0.,
        skip_tsk_tol_ratio = 0,
        grace_period_ratio = 0,
        skip_tasks_that_timeout = false,
        minworkers = Distributed.nworkers,
        maxworkers = Distributed.nworkers,
        nworkers = ()->Distributed.nprocs()-1,
        usemaster = false,
        quantum = ()->32,
        addprocs = Distributed.addprocs,
        init = epmap_default_init,
        preempt_channel_future = epmap_default_preempt_channel_future,
        checkpoint_task = epmap_default_checkpoint_task,
        restart_task = epmap_default_restart_task,
        reporttasks = true,
        keepcheckpoints = false,
        journalfile = "",
        journal_init_callback = tsks->nothing,
        journal_task_callback = tsk->nothing,
        # reduce specific
        reducer!::Function = default_reducer!,
        zeros = ()->nothing,
        scratch = ["/scratch"],
        id = randstring(6),
        epmapreduce_fetch = fetch,
        save_checkpoint = default_save_checkpoint,
        load_checkpoint = default_load_checkpoint,
        rm_checkpoint = default_rm_checkpoint,
        reduce_trigger = channel->nothing,
        save_partial_reduction = checkpoint->nothing,
        gethostname = gethostname,
        manager_event_forwarder = ch->nothing,
        manager_metrics = ()->(;),
        tracing::Union{TracingConfig, Nothing} = nothing)
    SchedulerOptions(
        retries,
        maxerrors,
        Float64(timeout_multiplier),
        Float64(timeout_function_multiplier),
        Float64(null_tsk_runtime_threshold),
        Float64(skip_tsk_tol_ratio),
        Float64(grace_period_ratio),
        skip_tasks_that_timeout,
        isa(minworkers, Function) ? minworkers : ()->minworkers,
        isa(maxworkers, Function) ? maxworkers : ()->maxworkers,
        nworkers,
        usemaster,
        isa(quantum, Function) ? quantum : ()->quantum,
        addprocs,
        init,
        preempt_channel_future,
        checkpoint_task,
        restart_task,
        reporttasks,
        keepcheckpoints,
        journalfile,
        journal_init_callback,
        journal_task_callback,
        reducer!,
        zeros,
        isa(scratch, AbstractArray) ? scratch : [scratch],
        id,
        epmapreduce_fetch,
        save_checkpoint,
        load_checkpoint,
        rm_checkpoint,
        reduce_trigger,
        save_partial_reduction,
        gethostname,
        manager_event_forwarder,
        manager_metrics,
        tracing)
end

function Base.copy(options::SchedulerOptions)
    SchedulerOptions(
        options.retries,
        options.maxerrors,
        options.timeout_multiplier,
        options.timeout_function_multiplier,
        options.null_tsk_runtime_threshold,
        options.skip_tsk_tol_ratio,
        options.grace_period_ratio,
        options.skip_tasks_that_timeout,
        options.minworkers,
        options.maxworkers,
        options.nworkers,
        options.usemaster,
        options.quantum,
        options.addprocs,
        options.init,
        options.preempt_channel_future,
        options.checkpoint_task,
        options.restart_task,
        options.reporttasks,
        options.keepcheckpoints,
        options.journalfile,
        options.journal_init_callback,
        options.journal_task_callback,
        options.reducer!,
        options.zeros,
        copy(options.scratch),
        options.id,
        options.epmapreduce_fetch,
        options.save_checkpoint,
        options.load_checkpoint,
        options.rm_checkpoint,
        options.reduce_trigger,
        options.save_partial_reduction,
        options.gethostname,
        options.manager_event_forwarder,
        options.manager_metrics,
        options.tracing)
end
