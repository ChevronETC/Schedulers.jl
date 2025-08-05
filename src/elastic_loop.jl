"""
ElasticLoop structure and operations for dynamic resource management.
"""

mutable struct ElasticLoop{
    FAddProcs<:Function,
    FInit<:Function,
    FMinWorkers<:Function,
    FMaxWorkers<:Function,
    FNWorkers<:Function,
    FTrigger<:Function,
    FSave<:Function,
    FQuantum<:Function,
    T,
    C,
}
    epmap_use_master::Bool
    initialized_pids::Set{Int}
    used_pids_map::Set{Int}
    used_pids_reduce::Set{Int}
    pid_channel_map_add::Channel{Int}
    pid_channel_map_remove::Channel{Tuple{Int,Bool}}
    pid_channel_reduce_add::Channel{Int}
    pid_channel_reduce_remove::Channel{Tuple{Int,Bool}}
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
    grace_period_start_time::Float64
    null_tsk_runtime_threshold::Float64
    skip_tsk_tol_ratio::Float64
    grace_period_ratio::Float64
end

function ElasticLoop(::Type{C}, tasks, options; isreduce) where {C}
    _tsk_pool_todo = vec(collect(tasks))

    eloop = ElasticLoop(
        options.usemaster,
        options.usemaster ? Set{Int}() : Set(1),
        options.usemaster ? Set{Int}() : Set(1),
        Set{Int}(),
        Channel{Int}(32),
        Channel{Tuple{Int,Bool}}(32),
        Channel{Int}(32),
        Channel{Tuple{Int,Bool}}(32),
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
        Inf,
        options.null_tsk_runtime_threshold,
        options.skip_tsk_tol_ratio,
        options.grace_period_ratio,
    )

    if !isreduce
        close(eloop.pid_channel_reduce_add)
        close(eloop.pid_channel_reduce_remove)
    end

    eloop
end

function reduce_checkpoints_is_dirty(eloop::ElasticLoop)
    for value in values(eloop.reduce_checkpoints_is_dirty)
        if value
            return value
        end
    end
    false
end

function save_partial_reduction(eloop)
    if isempty(eloop.reduce_checkpoints)
        @warn "reduction is empty, nothing to save."
    else
        try
            x = deserialize(eloop.reduce_checkpoints_snapshot[1])
            empty!(eloop.reduce_checkpoints_snapshot)
            eloop.epmap_save_partial_reduction(x)
        catch e
            @error "problem running user-supplied save_partial_reduction"
            logerror(e, Logging.Error)
        end
    end
end

trigger_reduction!(eloop::ElasticLoop) = put!(eloop.reduce_trigger_channel, true)

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

function reduce_trigger(eloop::ElasticLoop, journal, journal_task_callback)
    @debug "running user reduce trigger"
    try
        eloop.epmap_reduce_trigger(eloop)
    catch e
        @error "problem running user-supplied reduce trigger"
        logerror(e, Logging.Error)
    end

    @debug "checking for reduce trigger"
    if !isempty(eloop.reduce_trigger_channel)
        take!(eloop.reduce_trigger_channel)
        if !(eloop.is_reduce_triggered)
            eloop.is_reduce_triggered = true
            eloop.checkpoints_are_flushed = false
        end
    end

    @debug "eloop.is_reduce_triggered=$(eloop.is_reduce_triggered), length(eloop.checkpoints)=$(length(eloop.checkpoints)), length(eloop.reduce_checkpoints)=$(length(eloop.reduce_checkpoints))"
    if eloop.is_reduce_triggered &&
       eloop.checkpoints_are_flushed &&
       !reduce_checkpoints_is_dirty(eloop) &&
       length(eloop.reduce_checkpoints_snapshot) == 1
        @info "saving partial reduction, length(eloop.checkpoints)=$(length(eloop.checkpoints)), length(eloop.reduce_checkpoints)=$(length(eloop.reduce_checkpoints))"
        save_partial_reduction(eloop)
        @info "done saving partial reduction"
        try
            for tsk in eloop.tsk_pool_done
                if tsk ∉ eloop.tsk_pool_reduced
                    journal_stop!(
                        journal,
                        journal_task_callback;
                        stage = "reduced",
                        tsk,
                        pid = 0,
                        fault = false,
                    )
                end
            end
        catch e
            logerror(e, Logging.Warn)
        end
        eloop.tsk_pool_reduced = copy(eloop.tsk_pool_done)
        eloop.is_reduce_triggered = false
        eloop.checkpoints_are_flushed = true
    end
    eloop.is_reduce_triggered
end

function loop(eloop::ElasticLoop, journal, journal_task_callback, tsk_map, tsk_reduce)
    polling_interval = parse(Int, get(ENV, "SCHEDULERS_POLLING_INTERVAL", "1"))
    addrmprocs_timeout = parse(Int, get(ENV, "SCHEDULERS_ADDRMPROCS_TIMEOUT", "60"))

    tsk_addrmprocs = @async nothing
    tsk_addrmprocs_interrupt = @async nothing
    tsk_addrmprocs_tic = time()

    initializing_pids = Set{Int}()
    bad_pids = Set{Int}()
    wrkrs = Dict{Int,Union{Distributed.LocalProcess,Distributed.Worker}}()

    is_tasks_done_message_sent = false
    is_reduce_done_message_sent = false
    is_grace_period = false

    while true
        @debug "checking for interrupt=$(eloop.interrupted), error=$(eloop.errored)"
        yield()
        if eloop.interrupted
            put!(eloop.pid_channel_map_add, -1)
            if isopen(eloop.pid_channel_reduce_add)
                put!(eloop.pid_channel_reduce_add, -1)
            end
            if eloop.errored
                error("")
            end
            break
        end

        if (
            length(eloop.tsk_pool_done) / eloop.tsk_count > (1 - eloop.skip_tsk_tol_ratio)
        ) && !is_grace_period
            eloop.grace_period_start_time = time()
            is_grace_period = true
        end
        is_tasks_done = length(eloop.tsk_pool_done) == eloop.tsk_count
        is_more_tasks = length(eloop.tsk_pool_todo) > 0
        is_reduce_active =
            reduce_checkpoints_is_dirty(eloop) ||
            length(eloop.reduce_checkpoints) > 1 ||
            length(eloop.checkpoints) > 0
        is_more_checkpoints = length(eloop.reduce_checkpoints) > 1

        @debug "check for complete tasks when the number of completed tasks ($(length(eloop.tsk_pool_done))) equals the total number of tasks ($(eloop.tsk_count)))"
        yield()
        if is_tasks_done && !is_tasks_done_message_sent
            put!(eloop.pid_channel_map_add, -1)
            is_tasks_done_message_sent = true
        end

        @debug "check for complete reduction when tasks are done ($is_tasks_done) and reduce is not active ($(!is_reduce_active)), and there are no more checkpoints: ($(length(eloop.reduce_checkpoints)) pending, $(length(eloop.checkpoints)) active, $(length(filter(v->v, collect(values(eloop.reduce_checkpoints_is_dirty))))) dirty)"
        if is_tasks_done && !is_reduce_active
            @debug "elastic loop, check for open reduce channel, and if the reduce done message is already sent, isopen(eloop.pid_channel_reduce_add)=$(isopen(eloop.pid_channel_reduce_add)), is_reduce_done_message_sent=$is_reduce_done_message_sent"
            if isopen(eloop.pid_channel_reduce_add) && !is_reduce_done_message_sent
                @debug "elastic loop, putting -1 onto pid_channel_reduce_add, length(eloop.pid_channel_reduce_add.data)=$(length(eloop.pid_channel_reduce_add.data)))"
                put!(eloop.pid_channel_reduce_add, -1)
                @debug "elastic loop, done putting -1 onto pid_channel_reduce_add"
                is_reduce_done_message_sent = true

                @debug "recording reduced tasks"
                for tsk in eloop.tsk_pool_done
                    if tsk ∉ eloop.tsk_pool_reduced
                        journal_stop!(
                            journal,
                            journal_task_callback;
                            stage = "reduced",
                            tsk,
                            pid = 0,
                            fault = false,
                        )
                    end
                end
                eloop.tsk_pool_reduced = copy(eloop.tsk_pool_done)
            end
        end

        @debug "check for complete/failed tsk_map"
        yield()
        if istaskdone(tsk_map)
            @debug "map task done"
            isopen(eloop.pid_channel_map_add) && close(eloop.pid_channel_map_add)
            isopen(eloop.pid_channel_map_remove) && close(eloop.pid_channel_map_remove)
            if istaskfailed(tsk_map)
                @error "map task failed"
                if !istaskdone(tsk_reduce)
                    @async Base.throwto(tsk_reduce, InterruptException())
                end
                try
                    fetch(tsk_map)
                catch e
                    logerror(e, Logging.Error)
                end
                break
            end
        end

        @debug "check for failed reduce_map"
        if istaskfailed(tsk_reduce)
            @error "reduce task failed"
            if !istaskdone(tsk_map)
                @async Base.throwto(tsk_map, InterruptException())
            end
            fetch(tsk_reduce)
            break
        end

        @debug "check for complete map and reduce tasks, map: $(istaskdone(tsk_map)), reduce: $(istaskdone(tsk_reduce))"
        yield()
        if istaskdone(tsk_map) && istaskdone(tsk_reduce)
            isopen(eloop.pid_channel_reduce_add) && close(eloop.pid_channel_reduce_add)
            isopen(eloop.pid_channel_reduce_remove) &&
                close(eloop.pid_channel_reduce_remove)
            break
        end

        local _epmap_nworkers, _epmap_minworkers, _epmap_maxworkers, _epmap_quantum
        try
            _epmap_nworkers, _epmap_minworkers, _epmap_maxworkers, _epmap_quantum =
                eloop.epmap_nworkers(),
                eloop.epmap_minworkers(),
                eloop.epmap_maxworkers(),
                eloop.epmap_quantum()
        catch e
            @warn "problem in Schedulers.jl elastic loop when getting nworkers,minworkers,maxworkers,quantum"
            logerror(e)
            continue
        end

        if _epmap_minworkers > _epmap_maxworkers
            @warn "epmap_minworkers > epmap_maxworkers, setting epmap_minworkers to epmap_maxworkers"
            _epmap_minworkers = _epmap_maxworkers
        end

        uninitialized_pids = filter(
            pid->(pid ∉ initializing_pids && pid ∉ eloop.initialized_pids),
            workers(),
        )

        for uninitialized_pid in uninitialized_pids
            if !haskey(Distributed.map_pid_wrkr, uninitialized_pid)
                @warn "worker with pid=$uninitialized_pid is not registered"
            else
                wrkrs[uninitialized_pid] = Distributed.map_pid_wrkr[uninitialized_pid]
                push!(initializing_pids, uninitialized_pid)
                @async try
                    @debug "initializing failure count on $uninitialized_pid"
                    yield()
                    eloop.pid_failures[uninitialized_pid] = 0
                    @debug "loading modules on $uninitialized_pid"
                    yield()
                    load_modules_on_new_workers(uninitialized_pid)
                    @debug "loading functions on $uninitialized_pid"
                    yield()
                    load_functions_on_new_workers(uninitialized_pid)
                    @debug "calling init on new $uninitialized_pid"
                    yield()
                    eloop.epmap_init(uninitialized_pid)
                    @debug "done loading functions modules, and calling init on $uninitialized_pid"
                    yield()
                    _pid_up_timestamp[uninitialized_pid] = time()
                    push!(eloop.initialized_pids, uninitialized_pid)
                    pop!(initializing_pids, uninitialized_pid)
                catch e
                    @warn "problem initializing $uninitialized_pid, removing $uninitialized_pid from cluster."
                    logerror(e, Logging.Warn)
                    uninitialized_pid ∈ eloop.initialized_pids &&
                        pop!(eloop.initialized_pids, uninitialized_pid)
                    uninitialized_pid ∈ initializing_pids &&
                        pop!(initializing_pids, uninitialized_pid)
                    haskey(eloop.pid_failures, uninitialized_pid) &&
                        pop!(eloop.pid_failures, uninitialized_pid)
                    push!(bad_pids, uninitialized_pid)
                end
            end
        end

        free_pids = filter(
            pid->(
                pid ∈ eloop.initialized_pids &&
                pid ∉ eloop.used_pids_map &&
                pid ∉ eloop.used_pids_reduce &&
                pid ∉ bad_pids
            ),
            workers(),
        )

        @debug "workers()=$(workers()), free_pids=$free_pids, used_pids_map=$(eloop.used_pids_map), used_pids_reduce=$(eloop.used_pids_reduce), bad_pids=$bad_pids, initialized_pids=$(eloop.initialized_pids)"
        yield()

        @debug "checking for reduction trigger"
        reduce_trigger(eloop, journal, journal_task_callback)
        @debug "trigger=$(eloop.is_reduce_triggered)"

        for free_pid in free_pids
            if eloop.is_reduce_triggered &&
               !(eloop.checkpoints_are_flushed) &&
               length(eloop.checkpoints) == 0
                eloop.reduce_checkpoints_snapshot = copy(eloop.reduce_checkpoints)
                eloop.checkpoints_are_flushed = true
            end

            is_waiting_on_flush =
                eloop.is_reduce_triggered && !(eloop.checkpoints_are_flushed)
            @debug "is_reduce_triggered=$(eloop.is_reduce_triggered), checkpoints_are_flushed=$(eloop.checkpoints_are_flushed), is_waiting_on_flush=$is_waiting_on_flush, reduce_machine_count=$(length(eloop.used_pids_reduce))"

            wait_for_reduced_trigger =
                eloop.is_reduce_triggered &&
                div(length(eloop.reduce_checkpoints_snapshot), 2) >
                length(eloop.used_pids_reduce)
            if is_more_tasks && !is_waiting_on_flush && !wait_for_reduced_trigger
                @debug "putting pid=$free_pid onto map channel"
                push!(eloop.used_pids_map, free_pid)
                put!(eloop.pid_channel_map_add, free_pid)
            elseif is_more_checkpoints &&
                   !is_waiting_on_flush &&
                   div(length(eloop.reduce_checkpoints), 2) > length(eloop.used_pids_reduce)
                @debug "putting pid=$free_pid onto reduce channel"
                push!(eloop.used_pids_reduce, free_pid)
                put!(eloop.pid_channel_reduce_add, free_pid)
            end
        end

        # check to see if we need to add or remove machines
        δ, n_remaining_tasks = 0, 0
        try
            n_remaining_tasks =
                eloop.tsk_count - length(eloop.tsk_pool_done) +
                max(length(eloop.reduce_checkpoints) - 1, 0)
            δ = min(
                n_remaining_tasks - _epmap_nworkers,
                _epmap_maxworkers - _epmap_nworkers,
                _epmap_quantum,
            )
            # note that 1. we will only remove a machine if it is not in the 'eloop.used_pids' vector.
            # and 2. we will only remove machines if we are left with at least epmap_minworkers.
            if _epmap_nworkers + δ < _epmap_minworkers
                δ = min(_epmap_minworkers - _epmap_nworkers, _epmap_quantum)
            end
        catch e
            @warn "problem in Schedulers.jl elastic loop when computing the number of new machines to add"
            logerror(e, Logging.Warn)
        end

        @debug "add at most $_epmap_quantum machines when there are less than $_epmap_maxworkers, and there are less then the current task count: $n_remaining_tasks (δ=$δ, n=$_epmap_nworkers)"
        @debug "remove machine when there are more than $_epmap_minworkers, and less tasks ($n_remaining_tasks) than workers ($_epmap_nworkers), and when those workers are free (currently there are $_epmap_nworkers workers, $(length(eloop.used_pids_map)) map workers, and $(length(eloop.used_pids_reduce)) reduce workers)"

        if istaskdone(tsk_addrmprocs)
            try
                fetch(tsk_addrmprocs)
            catch e
                @warn "problem adding or removing processes"
                logerror(e, Logging.Warn)
            end

            if δ < 0 || length(bad_pids) > 0
                rm_pids = Int[]
                while !isempty(bad_pids)
                    push!(rm_pids, pop!(bad_pids))
                end
                δ += length(rm_pids)
                tsk_addrmprocs_tic = time()
                tsk_addrmprocs = @async begin
                    if δ < 0
                        free_pids = filter(
                            pid->pid ∉ eloop.used_pids_map && pid ∉ eloop.used_pids_reduce,
                            workers(),
                        )
                        push!(rm_pids, free_pids[1:min(-δ, length(free_pids))]...)
                    end
                    @debug "calling rmprocs on $rm_pids"
                    try
                        robust_rmprocs(rm_pids; waitfor = addrmprocs_timeout)
                    catch
                        @warn "unable to run rmprocs on $rm_pids in $addrmprocs_timeout seconds."
                    end

                    for rm_pid in rm_pids
                        haskey(wrkrs, rm_pid) && delete!(wrkrs, rm_pid)
                    end
                end
            elseif δ > 0
                try
                    @debug "adding $δ procs"
                    tsk_addrmprocs_tic = time()
                    tsk_addrmprocs = @async eloop.epmap_addprocs(δ)
                    sleep(2) # TODO: this seems needed for running with Distributed.SSHManager on a local cluster
                catch e
                    @error "problem adding new processes"
                    logerror(e, Logging.Error)
                end
            end
        elseif time() - tsk_addrmprocs_tic > addrmprocs_timeout+10 &&
               istaskdone(tsk_addrmprocs_interrupt)
            @warn "addprocs/rmprocs taking longer than expected, cancelling."
            tsk_addrmprocs_interrupt =
                @async Base.throwto(tsk_addrmprocs, InterruptException())
        end

        @debug "checking for workers sent from the map"
        yield()
        try
            while isready(eloop.pid_channel_map_remove)
                pid, isbad = take!(eloop.pid_channel_map_remove)
                @debug "map channel, received pid=$pid, making sure that it is initialized"
                yield()
                isbad && push!(bad_pids, pid)
                @debug "map channel, $pid is initialized, removing from used_pids"
                pid ∈ eloop.used_pids_map && pop!(eloop.used_pids_map, pid)
                @debug "map channel, done removing $pid from used_pids_map, used_pids_map=$(eloop.used_pids_map)"
            end
        catch e
            @warn "problem in Schedulers.jl elastic loop when removing workers from map"
            logerror(e, Logging.Warn)
        end

        @debug "checking for workers sent from the reduce"
        yield()
        try
            while isready(eloop.pid_channel_reduce_remove)
                pid, isbad = take!(eloop.pid_channel_reduce_remove)
                @debug "reduce channel, received pid=$pid (isbad=$isbad), making sure that it is initialized"
                yield()
                isbad && push!(bad_pids, pid)
                @debug "reduce_channel, $pid is initialied, removing from used_pids"
                pid ∈ eloop.used_pids_reduce && pop!(eloop.used_pids_reduce, pid)
                @debug "reduce channel, done removing $pid from used_pids, used_pids=$(eloop.used_pids_reduce)"
            end
        catch e
            @warn "problem in Schedulers.jl elastic loop when removing workers from reduce"
            logerror(e)
        end

        @debug "sleeping for $polling_interval seconds"
        sleep(polling_interval)
    end
    @debug "exited the elastic loop"

    @debug "cancel any pending add/rm procs task"
    while true
        istaskdone(tsk_addrmprocs) && break
        if time() - tsk_addrmprocs_tic > addrmprocs_timeout+10
            @warn "addprocs/rmprocs taking longer than expected, cancelling."
            @async Base.throwto(tsk_addrmprocs, InterruptException())
            break
        end
        sleep(polling_interval)
    end

    # ensure we are left with epmap_minworkers
    @debug "elastic loop, trimminig workers"
    try
        _workers = workers()
        if 1 ∈ _workers
            popfirst!(_workers)
        end
        n = length(_workers) - eloop.epmap_minworkers()
        if n > 0
            @debug "trimming $n workers"
            tsk_addrmprocs_tic = time()
            tsk_addrmprocs = robust_rmprocs(workers()[1:n]; waitfor = addrmprocs_timeout)
            @debug "done trimming $n workers"
        end
    catch e
        @warn "problem trimming workers after map-reduce"
        logerror(e, Logging.Warn)
    end
    @debug "elastic loop, done trimming workers"

    nothing
end
