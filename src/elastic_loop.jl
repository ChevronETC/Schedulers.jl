# --- LoopContext: mutable state local to the event loop ---

mutable struct LoopContext
    eloop::ElasticLoop
    journal::Dict{String,Any}
    journal_task_callback::Function
    tsk_map::Task
    tsk_reduce::Task
    initializing_pids::Set{Int}
    bad_pids::Set{Int}
    wrkrs::Dict{Int, Union{Distributed.LocalProcess, Distributed.Worker}}
    tsk_addrmprocs::Task
    tsk_addrmprocs_interrupt::Task
    tsk_addrmprocs_tic::Float64
    addrmprocs_timeout::Int
    pending_addprocs::Int
    is_tasks_done_message_sent::Bool
    is_reduce_done_message_sent::Bool
    is_grace_period::Bool
    should_stop::Bool
end

# --- Event handlers ---

function handle_event!(ctx::LoopContext, event::WorkerFreed)
    eloop = ctx.eloop
    if event.phase == :map
        event.pid ∈ eloop.used_pids_map && pop!(eloop.used_pids_map, event.pid)
    else
        event.pid ∈ eloop.used_pids_reduce && pop!(eloop.used_pids_reduce, event.pid)
    end
    event.is_bad && push!(ctx.bad_pids, event.pid)
    try_assign_worker!(ctx, event.pid)
end

function handle_event!(ctx::LoopContext, event::WorkerInitialized)
    pop!(ctx.initializing_pids, event.pid)
    push!(ctx.eloop.initialized_pids, event.pid)
    _pid_up_timestamp[event.pid] = time()
    ctx.pending_addprocs = max(0, ctx.pending_addprocs - 1)
    try_assign_worker!(ctx, event.pid)
end

function handle_event!(ctx::LoopContext, event::WorkerInitFailed)
    event.pid ∈ ctx.initializing_pids && pop!(ctx.initializing_pids, event.pid)
    event.pid ∈ ctx.eloop.initialized_pids && pop!(ctx.eloop.initialized_pids, event.pid)
    haskey(ctx.eloop.pid_failures, event.pid) && pop!(ctx.eloop.pid_failures, event.pid)
    push!(ctx.bad_pids, event.pid)
    ctx.pending_addprocs = max(0, ctx.pending_addprocs - 1)
end

function handle_event!(ctx::LoopContext, event::ScaleTick)
    check_interrupt!(ctx)
    ctx.should_stop && return
    check_grace_period!(ctx)
    check_tasks_done!(ctx)
    check_reduce_done!(ctx)
    discover_workers!(ctx)
    assign_free_workers!(ctx)
    check_reduce_trigger!(ctx)
    scale_workers!(ctx)
end

function handle_event!(ctx::LoopContext, event::MapPhaseCompleted)
    @debug "map phase completed"
    check_all_done!(ctx)
end

function handle_event!(ctx::LoopContext, event::MapPhaseFailed)
    @error "map task failed"
    if !istaskdone(ctx.tsk_reduce)
        @async Base.throwto(ctx.tsk_reduce, InterruptException())
    end
    try
        fetch(ctx.tsk_map)
    catch e
        @warn "failed tsk_map"
        logerror(e, Logging.Debug)
    end
    ctx.should_stop = true
end

function handle_event!(ctx::LoopContext, event::ReducePhaseCompleted)
    @debug "reduce phase completed"
    check_all_done!(ctx)
end

function handle_event!(ctx::LoopContext, event::ReducePhaseFailed)
    @error "reduce task failed"
    if !istaskdone(ctx.tsk_map)
        @async Base.throwto(ctx.tsk_map, InterruptException())
    end
    ctx.should_stop = true
end

function handle_event!(ctx::LoopContext, event::AddRmProcsCompleted)
    if !event.success
        ctx.pending_addprocs = 0
    end
end

function handle_event!(ctx::LoopContext, event::InterruptRequested)
    eloop = ctx.eloop
    put!(eloop.pid_channel_map_add, -1)
    isopen(eloop.pid_channel_reduce_add) && put!(eloop.pid_channel_reduce_add, -1)
    if eloop.errored
        ctx.should_stop = true
    end
end

function handle_event!(ctx::LoopContext, event::SchedulerEvent)
    @warn "unhandled scheduler event" type=typeof(event)
end

# --- Helper functions ---

function check_interrupt!(ctx)
    eloop = ctx.eloop
    if eloop.interrupted
        put!(eloop.pid_channel_map_add, -1)
        isopen(eloop.pid_channel_reduce_add) && put!(eloop.pid_channel_reduce_add, -1)
        ctx.should_stop = true
    end
end

function check_grace_period!(ctx)
    eloop = ctx.eloop
    if !ctx.is_grace_period && (length(eloop.tsk_pool_done) / eloop.tsk_count > (1 - eloop.skip_tsk_tol_ratio))
        eloop.grace_period_start_time = time()
        @debug "entering grace period"
        ctx.is_grace_period = true
    end
end

function check_tasks_done!(ctx)
    eloop = ctx.eloop
    is_tasks_done = length(eloop.tsk_pool_done) == eloop.tsk_count
    if is_tasks_done && !ctx.is_tasks_done_message_sent
        put!(eloop.pid_channel_map_add, -1)
        ctx.is_tasks_done_message_sent = true
    end
end

function check_reduce_done!(ctx)
    eloop = ctx.eloop
    is_tasks_done = length(eloop.tsk_pool_done) == eloop.tsk_count
    is_reduce_active = reduce_checkpoints_is_dirty(eloop) || length(eloop.reduce_checkpoints) > 1 || length(eloop.checkpoints) > 0

    if is_tasks_done && !is_reduce_active
        if isopen(eloop.pid_channel_reduce_add) && !ctx.is_reduce_done_message_sent
            put!(eloop.pid_channel_reduce_add, -1)
            ctx.is_reduce_done_message_sent = true

            for tsk in eloop.tsk_pool_done
                if tsk ∉ eloop.tsk_pool_reduced
                    journal_stop!(ctx.journal, ctx.journal_task_callback; stage="reduced", tsk, pid=0, fault=false)
                end
            end
            eloop.tsk_pool_reduced = copy(eloop.tsk_pool_done)
        end
    end
end

function discover_workers!(ctx)
    eloop = ctx.eloop
    all_pids = eloop.epmap_use_master ? procs() : workers()
    uninitialized_pids = filter(pid->(pid ∉ ctx.initializing_pids && pid ∉ eloop.initialized_pids), all_pids)

    for uninitialized_pid in uninitialized_pids
        if !haskey(Distributed.map_pid_wrkr, uninitialized_pid)
            @warn "worker with pid=$uninitialized_pid is not registered"
        else
            ctx.wrkrs[uninitialized_pid] = Distributed.map_pid_wrkr[uninitialized_pid]
            push!(ctx.initializing_pids, uninitialized_pid)
            @async try
                yield()
                eloop.pid_failures[uninitialized_pid] = 0
                yield()
                load_modules_on_new_workers(uninitialized_pid)
                yield()
                load_functions_on_new_workers(uninitialized_pid)
                yield()
                eloop.epmap_init(uninitialized_pid)
                yield()
                isopen(eloop.events) && put!(eloop.events, WorkerInitialized(uninitialized_pid))
            catch e
                @warn "problem initializing worker, removing from cluster" pid=uninitialized_pid exception=(e, catch_backtrace())
                isopen(eloop.events) && put!(eloop.events, WorkerInitFailed(uninitialized_pid))
            end
        end
    end
end

function assign_free_workers!(ctx)
    eloop = ctx.eloop
    all_pids = eloop.epmap_use_master ? procs() : workers()
    free_pids = filter(pid->(pid ∈ eloop.initialized_pids && pid ∉ eloop.used_pids_map && pid ∉ eloop.used_pids_reduce && pid ∉ ctx.bad_pids), all_pids)

    is_more_tasks = length(eloop.tsk_pool_todo) > 0
    is_more_checkpoints = length(eloop.reduce_checkpoints) > 1

    for free_pid in free_pids
        try_assign_worker!(ctx, free_pid; is_more_tasks, is_more_checkpoints)
    end
end

function try_assign_worker!(ctx, pid; is_more_tasks=nothing, is_more_checkpoints=nothing)
    eloop = ctx.eloop

    # Skip if worker is bad, not initialized, or already in use
    pid ∈ ctx.bad_pids && return
    pid ∉ eloop.initialized_pids && return
    (pid ∈ eloop.used_pids_map || pid ∈ eloop.used_pids_reduce) && return

    if is_more_tasks === nothing
        is_more_tasks = length(eloop.tsk_pool_todo) > 0
    end
    if is_more_checkpoints === nothing
        is_more_checkpoints = length(eloop.reduce_checkpoints) > 1
    end

    if eloop.is_reduce_triggered && !(eloop.checkpoints_are_flushed) && length(eloop.checkpoints) == 0
        eloop.reduce_checkpoints_snapshot = copy(eloop.reduce_checkpoints)
        eloop.checkpoints_are_flushed = true
    end

    is_waiting_on_flush = eloop.is_reduce_triggered && !(eloop.checkpoints_are_flushed)
    wait_for_reduced_trigger = eloop.is_reduce_triggered && div(length(eloop.reduce_checkpoints_snapshot), 2) > length(eloop.used_pids_reduce)

    if is_more_tasks && !is_waiting_on_flush && !wait_for_reduced_trigger
        push!(eloop.used_pids_map, pid)
        put!(eloop.pid_channel_map_add, pid)
    elseif is_more_checkpoints && !is_waiting_on_flush && div(length(eloop.reduce_checkpoints), 2) > length(eloop.used_pids_reduce)
        push!(eloop.used_pids_reduce, pid)
        put!(eloop.pid_channel_reduce_add, pid)
    end
end

function check_reduce_trigger!(ctx)
    reduce_trigger(ctx.eloop, ctx.journal, ctx.journal_task_callback)
end

function scale_workers!(ctx)
    eloop = ctx.eloop

    local _epmap_nworkers, _epmap_minworkers, _epmap_maxworkers, _epmap_quantum
    try
        _epmap_nworkers = eloop.epmap_nworkers()
        _epmap_minworkers = eloop.epmap_minworkers()
        _epmap_maxworkers = eloop.epmap_maxworkers()
        _epmap_quantum = eloop.epmap_quantum()
    catch e
        @warn "problem getting nworkers/minworkers/maxworkers/quantum"
        logerror(e, Logging.Debug)
        return
    end

    if _epmap_minworkers > _epmap_maxworkers
        _epmap_minworkers = _epmap_maxworkers
    end

    δ, n_remaining_tasks = 0, 0
    try
        n_remaining_tasks = eloop.tsk_count - length(eloop.tsk_pool_done) + max(length(eloop.reduce_checkpoints) - 1, 0)
        δ = min(n_remaining_tasks - _epmap_nworkers, _epmap_maxworkers - _epmap_nworkers, _epmap_quantum)
        if _epmap_nworkers + δ < _epmap_minworkers
            δ = min(_epmap_minworkers - _epmap_nworkers, _epmap_quantum)
        end
    catch e
        @warn "problem computing number of machines to add"
        logerror(e, Logging.Debug)
    end

    if istaskdone(ctx.tsk_addrmprocs)
        try
            fetch(ctx.tsk_addrmprocs)
        catch e
            @warn "problem adding or removing processes"
            logerror(e, Logging.Warn)
            ctx.pending_addprocs = 0
        end

        if δ < 0 || length(ctx.bad_pids) > 0
            rm_pids = Int[]
            while !isempty(ctx.bad_pids)
                push!(rm_pids, pop!(ctx.bad_pids))
            end
            δ += length(rm_pids)
            ctx.tsk_addrmprocs_tic = time()
            ctx.tsk_addrmprocs = @async begin
                if δ < 0
                    free_pids = filter(pid->pid ∉ eloop.used_pids_map && pid ∉ eloop.used_pids_reduce, workers())
                    push!(rm_pids, free_pids[1:min(-δ, length(free_pids))]...)
                end
                try
                    robust_rmprocs(rm_pids; waitfor=ctx.addrmprocs_timeout)
                catch e
                    @warn "unable to run rmprocs within timeout" pids=rm_pids timeout=ctx.addrmprocs_timeout exception=(e, catch_backtrace())
                end
                for rm_pid in rm_pids
                    haskey(ctx.wrkrs, rm_pid) && delete!(ctx.wrkrs, rm_pid)
                end
                put!(eloop.events, AddRmProcsCompleted(true))
            end
        elseif δ > 0 && ctx.pending_addprocs == 0
            try
                ctx.tsk_addrmprocs_tic = time()
                ctx.pending_addprocs = δ
                ctx.tsk_addrmprocs = @async begin
                    try
                        eloop.epmap_addprocs(δ)
                        put!(eloop.events, AddRmProcsCompleted(true))
                    catch e
                        @warn "addprocs failed" exception=(e, catch_backtrace())
                        put!(eloop.events, AddRmProcsCompleted(false))
                    end
                end
            catch e
                @error "problem adding new processes"
                logerror(e, Logging.Debug)
                ctx.pending_addprocs = 0
            end
        end
    elseif time() - ctx.tsk_addrmprocs_tic > ctx.addrmprocs_timeout + 10 && istaskdone(ctx.tsk_addrmprocs_interrupt)
        @warn "addprocs/rmprocs taking longer than expected, cancelling."
        ctx.tsk_addrmprocs_interrupt = @async Base.throwto(ctx.tsk_addrmprocs, InterruptException())
    end
end

function check_all_done!(ctx)
    if istaskdone(ctx.tsk_map) && istaskdone(ctx.tsk_reduce)
        eloop = ctx.eloop
        isopen(eloop.pid_channel_map_add) && close(eloop.pid_channel_map_add)
        isopen(eloop.pid_channel_reduce_add) && close(eloop.pid_channel_reduce_add)
        ctx.should_stop = true
    end
end

function cleanup_workers!(ctx)
    eloop = ctx.eloop
    addrmprocs_timeout = ctx.addrmprocs_timeout

    # Cancel any pending add/rm procs task
    while true
        istaskdone(ctx.tsk_addrmprocs) && break
        if time() - ctx.tsk_addrmprocs_tic > addrmprocs_timeout + 10
            @warn "addprocs/rmprocs taking longer than expected, cancelling."
            @async Base.throwto(ctx.tsk_addrmprocs, InterruptException())
            break
        end
        sleep(1)
    end

    # Trim to minworkers
    try
        _workers = workers()
        if 1 ∈ _workers
            popfirst!(_workers)
        end
        n = length(_workers) - eloop.epmap_minworkers()
        if n > 0
            robust_rmprocs(workers()[1:n]; waitfor=addrmprocs_timeout)
        end
    catch e
        @warn "problem trimming workers after map-reduce"
        logerror(e, Logging.Debug)
    end
end

# --- Main event-driven loop ---

function loop(eloop::ElasticLoop, journal, journal_task_callback, tsk_map, tsk_reduce)
    scaling_interval = parse(Float64, get(ENV, "SCHEDULERS_POLLING_INTERVAL", "1"))
    addrmprocs_timeout = parse(Int, get(ENV, "SCHEDULERS_ADDRMPROCS_TIMEOUT", "60"))

    ctx = LoopContext(
        eloop, journal, journal_task_callback, tsk_map, tsk_reduce,
        Set{Int}(),                                              # initializing_pids
        Set{Int}(),                                              # bad_pids
        Dict{Int, Union{Distributed.LocalProcess, Distributed.Worker}}(),  # wrkrs
        @async(nothing),                                         # tsk_addrmprocs
        @async(nothing),                                         # tsk_addrmprocs_interrupt
        time(),                                                  # tsk_addrmprocs_tic
        addrmprocs_timeout,                                      # addrmprocs_timeout
        0,                                                       # pending_addprocs
        false,                                                   # is_tasks_done_message_sent
        false,                                                   # is_reduce_done_message_sent
        false,                                                   # is_grace_period
        false,                                                   # should_stop
    )

    # --- Event producers ---

    # Periodic scaling/discovery tick
    scale_timer = Timer(scaling_interval; interval=scaling_interval) do _
        isopen(eloop.events) && put!(eloop.events, ScaleTick())
    end

    # Map phase watcher
    @async begin
        try
            wait(tsk_map)
            if istaskfailed(tsk_map)
                put!(eloop.events, MapPhaseFailed(nothing))
            else
                put!(eloop.events, MapPhaseCompleted())
            end
        catch e
            put!(eloop.events, MapPhaseFailed(e isa TaskFailedException ? e.task.result : e))
        end
    end

    # Reduce phase watcher
    @async begin
        try
            wait(tsk_reduce)
            if istaskfailed(tsk_reduce)
                put!(eloop.events, ReducePhaseFailed(nothing))
            else
                put!(eloop.events, ReducePhaseCompleted())
            end
        catch e
            put!(eloop.events, ReducePhaseFailed(e isa TaskFailedException ? e.task.result : e))
        end
    end

    # --- Event loop ---
    try
        for event in eloop.events
            handle_event!(ctx, event)
            ctx.should_stop && break
        end
    finally
        close(scale_timer)
        isopen(eloop.events) && close(eloop.events)
    end

    # If loop exited due to error, propagate
    if eloop.errored
        error("")
    end

    cleanup_workers!(ctx)

    nothing
end
