module Schedulers

using Dates, Distributed, JSON, Logging, Printf, Random, Serialization, Statistics

epmap_default_addprocs = n->addprocs(n)
epmap_default_preempt_channel_future = pid->nothing
epmap_default_checkpoint_task = pid->nothing
epmap_default_restart_task = pid->nothing
epmap_default_init = pid->nothing

function logerror(e, loglevel=Logging.Debug)
    io = IOBuffer()
    showerror(io, e)
    write(io, "\n\terror type: $(typeof(e))\n")
    if VERSION >= v"1.7"
        show(io, current_exceptions())
    else
        for (exc, bt) in Base.catch_stack()
            showerror(io, exc, bt)
            println(io)
        end
    end
    @logmsg loglevel String(take!(io))
end

# see https://github.com/JuliaTime/TimeZones.jl/issues/374
now_formatted() = Dates.format(now(Dates.UTC), dateformat"yyyy-mm-dd\THH:MM:SS\Z")

function journal_init(tsks, epmap_journal_init_callback; reduce)
    journal = reduce ? Dict{String,Any}("tasks"=>Dict(), "checkpoints"=>Dict(), "rmcheckpoints"=>Dict(), "pids"=>Dict()) : Dict{String,Any}("tasks"=>Dict())
    journal["start"] = now_formatted()
    for tsk in tsks
        journal["tasks"][tsk] = Dict("id"=>"$tsk", "trials"=>Dict[])
        if reduce
            journal["checkpoints"][tsk] = Dict("id"=>"$tsk", "trials"=>Dict[])
            journal["rmcheckpoints"][tsk] = Dict("id"=>"$tsk", "trials"=>Dict[])
        end
    end
    epmap_journal_init_callback(tsks)
    journal
end

function journal_final(journal)
    if haskey(journal, "pids")
        for pid in keys(journal["pids"])
            pop!(journal["pids"][pid], "tic")
        end
    end
    journal["done"] = now_formatted()
end

function journal_write(journal, filename)
    if !isempty(filename)
        write(filename, json(journal, 2))
    end
end

function journal_start!(journal, epmap_journal_task_callback=tsk->nothing; stage, tsk, pid, hostname)
    if stage ∈ ("tasks", "checkpoints", "rmcheckpoints")
        push!(journal[stage][tsk]["trials"],
            Dict(
                "pid" => pid,
                "hostname" => hostname,
                "start" => now_formatted()
            )
        )
    elseif stage ∈ ("restart", "reduce") && pid ∉ keys(journal["pids"])
        journal["pids"][pid] = Dict(
            "restart" => Dict(
                "elapsed" => 0.0,
                "faults" => 0
            ),
            "reduce" => Dict(
                "elapsed" => 0.0,
                "faults" => 0
            ),
            "hostname" => hostname,
            "tic" => time()
        )
    end

    if stage == "reduced"
        journal["tasks"][tsk]["trials"][end]["reduced"] = false
        journal["tasks"][tsk]["trials"][end]["reducedat"] = ""
    end

    if stage ∈ ("tasks", "reduced")
        epmap_journal_task_callback(journal["tasks"][tsk])
    end
end

function journal_stop!(journal, epmap_journal_task_callback=tsk->nothing; stage, tsk, pid, fault)
    if stage ∈ ("tasks", "checkpoints", "rmcheckpoints")
        journal[stage][tsk]["trials"][end]["status"] = fault ? "failed" : "succeeded"
        journal[stage][tsk]["trials"][end]["stop"] = now_formatted()
    elseif stage == "reduced"
        journal["tasks"][tsk]["trials"][end]["reduced"] = true
        journal["tasks"][tsk]["trials"][end]["reducedat"] = now_formatted()
    elseif stage ∈ ("restart", "reduce")
        journal["pids"][pid][stage]["elapsed"] += time() - journal["pids"][pid]["tic"]
        if fault
            journal["pids"][pid][stage]["faults"] += 1
        end
    end

    if stage ∈ ("tasks", "reduced")
        epmap_journal_task_callback(journal["tasks"][tsk])
    end
end

function load_modules_on_new_workers(pid)
    _names = names(Main; imported=true)
    for _name in _names
        try
            if isa(Base.eval(Main, _name), Module) && _name ∉ (:Base, :Core, :InteractiveUtils, :VSCodeServer, :Main, :_vscodeserver)
                remotecall_fetch(Base.eval, pid, Main, :(using $_name))
            end
        catch e
            @debug "caught error in load_modules_on_new_workers for module $_name"
            logerror(e, Logging.Debug)
        end
    end
    nothing
end

function load_functions_on_new_workers(pid)
    ignore = (Symbol("@enter"), Symbol("@run"), :ans, :eval, :include, :vscodedisplay)
    _names = filter(name->name ∉ ignore && isa(Base.eval(Main, name), Function), names(Main; imported=true))

    for _name in _names
        try
            remotecall_fetch(Base.eval, pid, Main, :(function $_name end))
        catch e
            @debug "caught error in load_functions_on_new_workers (function) for pid '$pid' and function '$_name'"
            logerror(e, Logging.Debug)
        end
    end

    for _name in _names
        for method in Base.eval(Main, :(methods($_name)))
            try
                remotecall_fetch(Base.eval, pid, Main, :($method))
            catch e
                @debug "caught error in load_functions_on_new_workers (methods) for pid '$pid', function '$_name', method '$method'"
                logerror(e, Logging.Debug)
            end
        end
    end
    nothing
end

# for performance metrics, track when the pid is started
const _pid_up_timestamp = Dict{Int, Float64}()

mutable struct ElasticLoop{FAddProcs<:Function,FInit<:Function,FMinWorkers<:Function,FMaxWorkers<:Function,FNWorkers<:Function,FTrigger<:Function,FSave<:Function,FQuantum<:Function,T,C}
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

struct TimeoutException <: Exception
    pid::Int
    elapsed::Float64
end

maximum_task_time(tsk_times, tsk_count, timeout_multiplier) = length(tsk_times) > max(0, floor(Int, 0.5*tsk_count)) ? maximum(tsk_times)*timeout_multiplier : Inf

struct PreemptException <: Exception end

function default_threadpool_checkpoint_call(preempt_channel_future, checkpoint_task, restart_task, tsk, f, args...; kwargs...)
    # see: https://github.com/JuliaLang/julia/issues/53217
    # We explicitly use a default thread here.  If we don't do this, then the work is sent to the
    # interactive thread pool, and we do not want to block an interactive thread.  For example,
    # in AzManagers.jl we use the interactive thread pool to poll for spot evictions.
    t = Threads.@spawn begin
        try
            restart_task(tsk)
        catch
            @warn "error restarting task $tsk"
            logerror(e, Logging.Warn)
        end
        f(args...; kwargs...)
    end

    if preempt_channel_future !== nothing
        preempt_channel = fetch(preempt_channel_future)::Channel{Bool}
        # this loop runs on the interactive thread
        while !istaskdone(t)
            if isready(preempt_channel)
                take!(preempt_channel)
                try
                    checkpoint_task(tsk)
                catch e
                    @warn "error checkpointing task $tsk"
                    logerror(e, Logging.Warn)
                end
                @async Base.throwto(t, InterruptException())
                throw(PreemptException())
            end
            sleep(0.1)
        end
    end

    fetch(t)
end

function remotecall_wait_timeout(tsk_times, tsk_count, timeout_multiplier, preempt_channel_future, checkpoint_task, restart_task, tsk, f, pid, args...; kwargs...)
    t = @async remotecall_wait(default_threadpool_checkpoint_call, pid, preempt_channel_future, checkpoint_task, restart_task, tsk, f, args...; kwargs...)
    tic = time()
    while !istaskdone(t)
        if time() - tic > maximum_task_time(tsk_times, tsk_count, timeout_multiplier)
            throw(TimeoutException(pid, time() - tic))
        end
        sleep(1)
    end
    isa(tsk_times, AbstractArray) && push!(tsk_times, time() - tic)
    if istaskfailed(t)
        fetch(t)
    end
    nothing
end

function remotecall_fetch_timeout(tsk_times, tsk_count, timeout_multiplier, preempt_channel_future, checkpoint_task, restart_task, tsk, f, pid, args...; kwargs...)
    t = @async remotecall_fetch(default_threadpool_checkpoint_call, pid, preempt_channel_future, checkpoint_task, restart_task, tsk, f, args...; kwargs...)
    tic = time()
    while !istaskdone(t)
        if tic - time() > maximum_task_time(tsk_times, tsk_count, timeout_multiplier)
            throw(TimeoutException(pid, time() - tic))
        end
        sleep(1)
    end
    isa(tsk_times, AbstractArray) && push!(tsk_times, time() - tic)
    fetch(t)
end

function robust_average(tsk_times, null_tsk_runtime_threshold, tsk_count)
    tsk_times_robust = filter(tsk_time->tsk_time > null_tsk_runtime_threshold, tsk_times)
    if length(tsk_times_robust) >= 0.3 * tsk_count     # Start statistics after 30% of the tasks are done
        return sum(tsk_times_robust) / length(tsk_times_robust)
    else
        return Inf
    end
end

function check_timeout_status(tic, tsk_times, tsk_count, timeout_function_multiplier, grace_period_start_time, null_tsk_runtime_threshold, grace_period_ratio)
    toc = time()
    average_task_time = robust_average(tsk_times, null_tsk_runtime_threshold, tsk_count)
    if toc - tic > average_task_time * timeout_function_multiplier
        is_timeout = true
    elseif toc - grace_period_start_time > grace_period_ratio * average_task_time
        is_timeout = true
    else
        is_timeout = false
    end
    return is_timeout
end

function remotecall_func_wait_timeout(tsk_times, eloop, options, preempt_channel_future, checkpoint_task, restart_task, tsk, f, pid, args...; kwargs...)
    t = @async remotecall_wait(default_threadpool_checkpoint_call, pid, preempt_channel_future, checkpoint_task, restart_task, tsk, f, args...; kwargs...)
    tic = time()
    while !istaskdone(t)
        if check_timeout_status(tic, tsk_times, eloop.tsk_count, options.timeout_function_multiplier, eloop.grace_period_start_time, options.null_tsk_runtime_threshold, options.grace_period_ratio)
            throw(TimeoutException(pid, time() - tic))
        end
        sleep(1)
    end
    isa(tsk_times, AbstractArray) && push!(tsk_times, time() - tic)
    if istaskfailed(t)
        fetch(t)
    end
    nothing
end

remotecall_default_threadpool(f, pid, args...; kwargs...) = remotecall(default_threadpool_checkpoint_call, pid, nothing, tsk->nothing, tsk->nothing, 0, f, args...; kwargs...)

function handle_exception(e::PreemptException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    @warn "preempt exception caught on process with id=$pid ($hostname)"
    (bad_pid = true, do_break=true, do_interrupt=false, do_error=true)
end

function handle_exception(e::TimeoutException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    logerror(e, Logging.Warn)

    fails[pid] += 1
    nerrors = sum(values(fails))

    # If the task times out than we make the conservative assumption that there might be something
    # wrong with the machine it is running on, hence we set bad_pid=true.
    r = (bad_pid = true, do_break=true, do_interrupt=false, do_error=false)

    if nerrors >= epmap_maxerrors
        @error "too many total errors, $nerrors errors"
        r = (bad_pid = true, do_break=true, do_interrupt=true, do_error=true)
    end

    r
end

function handle_exception(e, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    logerror(e, Logging.Warn)

    fails[pid] += 1
    nerrors = sum(values(fails))

    r = (bad_pid = false, do_break=false, do_interrupt=false, do_error=false)

    if isa(e, InterruptException)
        r = (bad_pid = false, do_break=false, do_interrupt=true, do_error=false)
    elseif isa(e, ProcessExitedException)
        @warn "process with id=$pid ($hostname) exited, marking as bad for removal."
        r = (bad_pid = true, do_break=true, do_interrupt=false, do_error=false)
    elseif fails[pid] > epmap_retries
        @warn "too many failures on process with id=$pid ($hostname), removing from process list"
        r = (bad_pid = true, do_break=true, do_interrupt=false, do_error=false)
    end

    if nerrors >= epmap_maxerrors
        @error "too many total errors, $nerrors errors"
        r = (bad_pid = false, do_break=true, do_interrupt=true, do_error=true)
    end

    r
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
    if eloop.is_reduce_triggered && eloop.checkpoints_are_flushed && !reduce_checkpoints_is_dirty(eloop) && length(eloop.reduce_checkpoints_snapshot) == 1
        @info "saving partial reduction, length(eloop.checkpoints)=$(length(eloop.checkpoints)), length(eloop.reduce_checkpoints)=$(length(eloop.reduce_checkpoints))"
        save_partial_reduction(eloop)
        @info "done saving partial reduction"
        try
            for tsk in eloop.tsk_pool_done
                if tsk ∉ eloop.tsk_pool_reduced
                    journal_stop!(journal, journal_task_callback; stage="reduced", tsk, pid=0, fault=false)
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

function robust_rmprocs(pids; waitfor)
    try
        rmprocs(pids; waitfor)
    catch e
        @warn "unable to run rmprocs on $pids, using fall-back strategy"
        logerror(e, Logging.Warn)
        try
            rmprocset = Union{Distributed.LocalProcess, Distributed.Worker}[]
            for pid in pids
                if pid != 1 && haskey(Distributed.map_pid_wrkr, pid)
                    w = Distributed.map_pid_wrkr[pid]
                    push!(rmprocset, w)
                end
            end
            unremoved = [wrkr.id for wrkr in filter(w -> w.state !== Distributed.W_TERMINATED, rmprocset)]

            lock(Distributed.worker_lock)
            try
                for pid in unremoved
                    @debug "robust_rmprocs, setting worker state, and calling kill"
                    if haskey(Distributed.map_pid_wrkr, pid)
                        w = Distributed.map_pid_wrkr[pid]
                        Distributed.set_worker_state(w, Distributed.W_TERMINATED)
                        Distributed.deregister_worker(pid)
                        Distributed.kill(w.manager, pid, w.config)
                    end
                end
            catch
            finally
                unlock(Distributed.worker_lock)
            end
        catch e
            @warn "rmprocs fall-back strategy failed."
            logerror(e, Logging.Error)
        end
    end
end

function loop(eloop::ElasticLoop, journal, journal_task_callback, tsk_map, tsk_reduce)
    polling_interval = parse(Int, get(ENV, "SCHEDULERS_POLLING_INTERVAL", "1"))
    addrmprocs_timeout = parse(Int, get(ENV, "SCHEDULERS_ADDRMPROCS_TIMEOUT", "60"))

    tsk_addrmprocs = @async nothing
    tsk_addrmprocs_interrupt = @async nothing
    tsk_addrmprocs_tic = time()

    initializing_pids = Set{Int}()
    bad_pids = Set{Int}()
    wrkrs = Dict{Int, Union{Distributed.LocalProcess, Distributed.Worker}}()

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

        if (length(eloop.tsk_pool_done) / eloop.tsk_count > (1 - eloop.skip_tsk_tol_ratio)) && !is_grace_period
            eloop.grace_period_start_time = time()
            is_grace_period = true
        end
        is_tasks_done = length(eloop.tsk_pool_done) == eloop.tsk_count
        is_more_tasks = length(eloop.tsk_pool_todo) > 0
        is_reduce_active = reduce_checkpoints_is_dirty(eloop) || length(eloop.reduce_checkpoints) > 1 || length(eloop.checkpoints) > 0
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
                        journal_stop!(journal, journal_task_callback; stage="reduced", tsk, pid=0, fault=false)
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
            isopen(eloop.pid_channel_reduce_remove) && close(eloop.pid_channel_reduce_remove)
            break
        end
        
        local _epmap_nworkers,_epmap_minworkers,_epmap_maxworkers,_epmap_quantum
        try
            _epmap_nworkers,_epmap_minworkers,_epmap_maxworkers,_epmap_quantum = eloop.epmap_nworkers(),eloop.epmap_minworkers(),eloop.epmap_maxworkers(),eloop.epmap_quantum()
        catch e
            @warn "problem in Schedulers.jl elastic loop when getting nworkers,minworkers,maxworkers,quantum"
            logerror(e)
            continue
        end

        if _epmap_minworkers > _epmap_maxworkers
            @warn "epmap_minworkers > epmap_maxworkers, setting epmap_minworkers to epmap_maxworkers"
            _epmap_minworkers = _epmap_maxworkers
        end

        uninitialized_pids = filter(pid->(pid ∉ initializing_pids && pid ∉ eloop.initialized_pids), workers())

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
                    uninitialized_pid ∈ eloop.initialized_pids && pop!(eloop.initialized_pids, uninitialized_pid)
                    uninitialized_pid ∈ initializing_pids && pop!(initializing_pids, uninitialized_pid)
                    haskey(eloop.pid_failures, uninitialized_pid) && pop!(eloop.pid_failures, uninitialized_pid)
                    push!(bad_pids, uninitialized_pid)
                end
            end
        end

        free_pids = filter(pid->(pid ∈ eloop.initialized_pids && pid ∉ eloop.used_pids_map && pid ∉ eloop.used_pids_reduce && pid ∉ bad_pids), workers())

        @debug "workers()=$(workers()), free_pids=$free_pids, used_pids_map=$(eloop.used_pids_map), used_pids_reduce=$(eloop.used_pids_reduce), bad_pids=$bad_pids, initialized_pids=$(eloop.initialized_pids)"
        yield()

        @debug "checking for reduction trigger"
        reduce_trigger(eloop, journal, journal_task_callback)
        @debug "trigger=$(eloop.is_reduce_triggered)"

        for free_pid in free_pids
            if eloop.is_reduce_triggered && !(eloop.checkpoints_are_flushed) && length(eloop.checkpoints) == 0
                eloop.reduce_checkpoints_snapshot = copy(eloop.reduce_checkpoints)
                eloop.checkpoints_are_flushed = true
            end

            is_waiting_on_flush = eloop.is_reduce_triggered && !(eloop.checkpoints_are_flushed)
            @debug "is_reduce_triggered=$(eloop.is_reduce_triggered), checkpoints_are_flushed=$(eloop.checkpoints_are_flushed), is_waiting_on_flush=$is_waiting_on_flush, reduce_machine_count=$(length(eloop.used_pids_reduce))"

            wait_for_reduced_trigger = eloop.is_reduce_triggered && div(length(eloop.reduce_checkpoints_snapshot), 2) > length(eloop.used_pids_reduce)
            if is_more_tasks && !is_waiting_on_flush && !wait_for_reduced_trigger
                @debug "putting pid=$free_pid onto map channel"
                push!(eloop.used_pids_map, free_pid)
                put!(eloop.pid_channel_map_add, free_pid)
            elseif is_more_checkpoints && !is_waiting_on_flush && div(length(eloop.reduce_checkpoints), 2) > length(eloop.used_pids_reduce)
                @debug "putting pid=$free_pid onto reduce channel"
                push!(eloop.used_pids_reduce, free_pid)
                put!(eloop.pid_channel_reduce_add, free_pid)
            end
        end

        # check to see if we need to add or remove machines
        δ,n_remaining_tasks = 0,0
        try
            n_remaining_tasks = eloop.tsk_count - length(eloop.tsk_pool_done) + max(length(eloop.reduce_checkpoints) - 1, 0)
            δ = min(n_remaining_tasks - _epmap_nworkers, _epmap_maxworkers - _epmap_nworkers, _epmap_quantum)
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
                        free_pids = filter(pid->pid ∉ eloop.used_pids_map && pid ∉ eloop.used_pids_reduce, workers())
                        push!(rm_pids, free_pids[1:min(-δ, length(free_pids))]...)
                    end
                    @debug "calling rmprocs on $rm_pids"
                    try
                        robust_rmprocs(rm_pids; waitfor=addrmprocs_timeout)
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
        elseif time() - tsk_addrmprocs_tic > addrmprocs_timeout+10 && istaskdone(tsk_addrmprocs_interrupt)
            @warn "addprocs/rmprocs taking longer than expected, cancelling."
            tsk_addrmprocs_interrupt = @async Base.throwto(tsk_addrmprocs, InterruptException())
        end

        @debug "checking for workers sent from the map"
        yield()
        try
            while isready(eloop.pid_channel_map_remove)
                pid,isbad = take!(eloop.pid_channel_map_remove)
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
                pid,isbad = take!(eloop.pid_channel_reduce_remove)
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
            tsk_addrmprocs = robust_rmprocs(workers()[1:n]; waitfor=addrmprocs_timeout)
            @debug "done trimming $n workers"
        end
    catch e
        @warn "problem trimming workers after map-reduce"
        logerror(e, Logging.Warn)
    end
    @debug "elastic loop, done trimming workers"

    nothing
end

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
        gethostname = gethostname)
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
        gethostname)
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
        options.gethostname)
end

"""
    epmap([options,] f, tasks, args...; kwargs...)

where `f` is the map function, and `tasks` is an iterable collection of tasks.  The function `f`
takes the positional arguments `args`, and the keyword arguments `kwargs`.  `epmap` is parameterized
using `options::SchedulerOptions` and can be built using `options=SchedulerOptions(;epmap_kwargs...)`
and `pmap_kwargs` are as follows.

## epmap_kwargs
* `retries=0` number of times to retry a task on a given machine before removing that machine from the cluster
* `maxerrors=typemax(Int)` the maximum number of errors before we give-up and exit
* `timeout_multiplier=5` if any (miscellaneous) task takes `timeout_multiplier` longer than the mean (miscellaneous) task time, then abort that task
* `timeout_function_multiplier=5` if any (actual) task takes `timeout_function_multiplier` longer than the (robust) mean (actual) task time, then abort that task
* `null_tsk_runtime_threshold=0` the maximum duration (in seconds) for a task to be considered insignificant or ‘null’ and thus not included in the timeout measurement.
* `skip_tsk_tol_ratio=0` the ratio of the total number of tasks that can be skipped
* `grace_period_ratio=0` the ratio between the "grace period" (when enough number of tasks are done) over the (robust) average task time
* `skip_tasks_that_timeout=false` skip task that exceed the timeout, or retry them on a different machine
* `minworkers=Distributed.nworkers` method (or value) giving the minimum number of workers to elastically shrink to
* `maxworkers=Distributed.nworkers` method (or value) giving the maximum number of workers to elastically expand to
* `usemaster=false` assign tasks to the master process?
* `nworkers=Distributed.nworkers` the number of machines currently provisioned for work[1]
* `quantum=()->32` the maximum number of workers to elastically add at a time
* `addprocs=n->Distributed.addprocs(n)` method for adding n processes (will depend on the cluster manager being used)
* `init=pid->nothing` after starting a worker, this method is run on that worker.
* `preempt_channel_future=pid->nothing` method for retrieving a `Future` that hold a `Channel` through which preemption events are communicated[2].
* `checkpont_task=tsk->nothing` method that will be run if a preemption event is communicated.
* `restart_task=tsk->nothing` method that will be run at the start of a task, and can be used for partially completed tasks that have checkpoint information.
* `reporttasks=true` log task assignment
* `journal_init_callback=tsks->nothing` additional method when intializing the journal
* `journal_task_callback=tsk->nothing` additional method when journaling a task
# GXTODO: add doc
## Notes
[1] The number of machines provisioned may be greater than the number of workers in the cluster since with
some cluster managers, there may be a delay between the provisioining of a machine, and when it is added to the
Julia cluster.
[2] For example, on Azure Cloud a SPOT instance will be pre-empted if someone is willing to pay more for it
"""
function epmap(options::SchedulerOptions, f::Function, tasks, args...; kwargs...)
    eloop = ElasticLoop(Nothing, tasks, options; isreduce=false)
    journal = journal_init(tasks, options.journal_init_callback; reduce=false)
    
    tsk_map = @async epmap_map(options, f, tasks, eloop, journal, args...; kwargs...)
    loop(eloop, journal, options.journal_task_callback, tsk_map, @async nothing)
    fetch(tsk_map)
end

function epmap_map(options::SchedulerOptions, f::Function, tasks, eloop::ElasticLoop, journal, args...; kwargs...)
    tsk_times = Float64[]

    # work loop
    @sync while true
        eloop.interrupted && break
        pid = take!(eloop.pid_channel_map_add)

        @debug "pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.

        hostname = ""

        local preempt_channel_future
        try
            preempt_channel_future = options.preempt_channel_future(pid)
        catch
            @warn "failed to retreive preempt_channel_future.  checkpoint/restart functionality disabled."
            preempt_channel_future = nothing
        end

        @async try
            while true
                if hostname == ""
                    try
                        hostname = remotecall_fetch_timeout(60, 1, 1, nothing, tsk->nothing, tsk->nothing, 0, options.gethostname, pid)
                    catch e
                        @warn "unable to determine hostname for pid=$pid within 60 seconds"
                        logerror(e, Logging.Warn)
                        put!(eloop.pid_channel_map_remove, (pid, true))
                        break
                    end
                end

                @debug "map task loop exit condition" pid length(eloop.tsk_pool_todo) eloop.interrupted
                if length(eloop.tsk_pool_todo) == 0 || eloop.interrupted
                    @debug "putting $pid onto map_remove channel"
                    put!(eloop.pid_channel_map_remove, (pid,false))
                    break
                end
                isempty(eloop.tsk_pool_todo) && (yield(); continue)

                local tsk
                try
                    tsk = popfirst!(eloop.tsk_pool_todo)
                catch
                    # just in case another task does popfirst! before us (unlikely)
                    yield()
                    continue
                end

                try
                    options.reporttasks && @info "running task $tsk on process $pid ($hostname); $(nworkers()) workers total; $(length(eloop.tsk_pool_todo)) tasks left in task-pool."
                    yield()
                    journal_start!(journal, options.journal_task_callback; stage="tasks", tsk, pid, hostname)
                    remotecall_func_wait_timeout(tsk_times, eloop, options, preempt_channel_future, options.checkpoint_task, options.restart_task, tsk, f, pid, tsk, args...; kwargs...)
                    journal_stop!(journal, options.journal_task_callback; stage="tasks", tsk, pid, fault=false)
                    push!(eloop.tsk_pool_done, tsk)
                    @debug "...pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(eloop.tsk_pool_todo), tsk_pool_done=$(eloop.tsk_pool_done) -!"
                    yield()
                catch e
                    @warn "caught an exception, there have been $(eloop.pid_failures[pid]) failure(s) on process $pid ($hostname)..."
                    journal_stop!(journal, options.journal_task_callback; stage="tasks", tsk, pid, fault=true)
                    if isa(e, TimeoutException) && options.skip_tasks_that_timeout
                        @warn "skipping task '$tsk' that timed out"
                        push!(eloop.tsk_pool_done, tsk)
                        push!(eloop.tsk_pool_timed_out, tsk)
                    else
                        push!(eloop.tsk_pool_todo, tsk)
                    end
                    r = handle_exception(e, pid, hostname, eloop.pid_failures, options.maxerrors, options.retries)
                    if r.do_break || r.do_interrupt
                        put!(eloop.pid_channel_map_remove, (pid,r.bad_pid))
                    end
                    eloop.interrupted = r.do_interrupt
                    eloop.errored = r.do_error
                    r.do_break && break
                end
            end
        catch e
            @warn "uncaught exception in worker loop for pid=$pid"
            logerror(e, Logging.Warn)
            @debug "putting $pid onto remove channel"
            isopen(eloop.pid_channel_map_remove) && put!(eloop.pid_channel_map_remove, (pid,true))
            @debug "done putting $pid onto remove channel"
        end
    end
    @debug "exiting the map loop"

    journal_final(journal)

    journal, eloop.tsk_pool_timed_out
end

epmap(f::Function, tasks, args...; kwargs...) = epmap(SchedulerOptions(), f, tasks, args..., kwargs...)

"""
    epmapreduce!(result, [options], f, tasks, args...; kwargs...) -> result

where `f` is the map function, and `tasks` are an iterable set of tasks to map over.  The
positional arguments `args` and the named arguments `kwargs` are passed to `f` which has
the method signature: `f(localresult, f, task, args; kwargs...)`.  `localresult` is
the assoicated partial reduction contribution to `result`.  `epmapreduce!` is parameterized
by `options::SchedulerOptions` and can be built using `options=SchedulerOptions(;epmap_kwargs...)`
and `epmap_kwargs` are as follows.

## epmap_kwargs
* `reducer! = default_reducer!` the method used to reduce the result. The default is `(x,y)->(x .+= y)`
* `save_checkpoint = default_save_checkpoint` the method used to save checkpoints[1]
* `load_checkpoint = default_load_checkpoint` the method used to load a checkpoint[2]
* `zeros = ()->zeros(eltype(result), size(result))` the method used to initiaize partial reductions
* `retries=0` number of times to retry a task on a given machine before removing that machine from the cluster
* `maxerrors=Inf` the maximum number of errors before we give-up and exit
* `timeout_multiplier=5` if any (miscellaneous) task takes `timeout_multiplier` longer than the mean (miscellaneous) task time, then abort that task
* `timeout_function_multiplier=5` if any (actual) task takes `timeout_function_multiplier` longer than the (robust) mean (actual) task time, then abort that task
* `null_tsk_runtime_threshold=0` the maximum duration (in seconds) for a task to be considered insignificant or ‘null’ and thus not included in the timeout measurement.
* `skip_tsk_tol_ratio=0` the ratio of the total number of tasks that can be skipped
* `grace_period_ratio=0` the ratio between the "grace period" (when enough number of tasks are done) over the (robust) average task time
* `skip_tasks_that_timeout=false` skip task that exceed the timeout, or retry them on a different machine
* `minworkers=nworkers` method giving the minimum number of workers to elastically shrink to
* `maxworkers=nworkers` method giving the maximum number of workers to elastically expand to
* `usemaster=false` assign tasks to the master process?
* `nworkers=nworkers` the number of machines currently provisioned for work[3]
* `quantum=()->32` the maximum number of workers to elastically add at a time
* `addprocs=n->addprocs(n)` method for adding n processes (will depend on the cluster manager being used)
* `init=pid->nothing` after starting a worker, this method is run on that worker.
* `preempt_channel_future=pid->nothing` method for retrieving a `Future` that hold a `Channel` through which preemption events are communicated.
* `checkpont_task=tsk->nothing` method that will be run if a preemption event is communicated.
* `restart_task=tsk->nothing` method that will be run at the start of a task, and can be used for partially completed tasks that have checkpoint information.
* `scratch=["/scratch"]` storage location accesible to all cluster machines (e.g NFS, Azure blobstore,...)[4]
* `reporttasks=true` log task assignment
* `journalfile=""` write a journal showing what was computed where to a json file
* `journal_init_callback=tsks->nothing` additional method when intializing the journal
* `journal_task_callback=tsk->nothing` additional method when journaling a task
* `id=randstring(6)` identifier used for the scratch files
* `reduce_trigger=eloop->nothing` condition for triggering a reduction prior to the completion of the map
* `save_partial_reduction=x->nothing` method for saving a partial reduction triggered by `reduce_trigger`
## Notes
[1] The signiture is `my_save_checkpoint(checkpoint_file, x)` where `checkpoint_file` is the file that will be
written to, and `x` is the data that will be written.
[2] The signiture is `my_load_checkpoint(checkpoint_file)` where `checkpoint_file` is the file that
data will be loaded from.
[3] The number of machines provisioined may be greater than the number of workers in the cluster since with
some cluster managers, there may be a delay between the provisioining of a machine, and when it is added to the
Julia cluster.
[4] If more than one scratch location is selected, then check-point files will be distributed across those locations.
This can be useful if you are, for example, constrained by cloud storage through-put limits.

# Examples
## Example 1
With the assumption that `/scratch` is accesible from all workers:
```
using Distributed
addprocs(2)
@everywhere using Distributed, Schedulers
@everywhere f(x, tsk) = x .+= tsk; nothing)
result,tsks = epmapreduce!(zeros(Float32,10), f, 1:100)
rmprocs(workers())
```

## Example 2
Using Azure blob storage:
```
using Distributed, AzStorage
container = AzContainer("scratch"; storageaccount="mystorageaccount")
mkpath(container)
addprocs(2)
@everywhere using Distributed, Schedulers
@everywhere f(x, tsk) = (x .+= tsk; nothing)
result,tsks = epmapreduce!(zeros(Float32,10), SchedulerOptions(;scratch=container), f, 1:100)
rmprocs(workers())
```

## Example 3
With a reduce trigger every 10 minutes:
```
function my_reduce_trigger(eloop, tic)
    if time() - tic[] > 600
        trigger_reduction!(eloop)
        tic[] = time()
    end
end

my_save(r, filename) = write(filename, r)

tic = Ref(time())
addprocs(2)
@everywhere f(x, tsk) = (x .+= tsk; nothing)
@everywhere using Distributed, Schedulers
result,tsks = epmapreduce!(zeros(Float32,10), SchedulerOptions(;reduce_trigger=eloop->my_reduce_trigger(eloop, tic), save_partial_reduction=r->my_save(r, "partial.bin"), f, 1:100)
```
Note that the methods `complete_tasks`, `pending_tasks`, `reduced_tasks`, and `total_tasks` can be useful when designing the `reduce_trigger` method.
"""
function epmapreduce!(result::T, options::SchedulerOptions, f::Function, tasks, args...; kwargs...) where {T}
    options_init = copy(options)

    local epmap_eloop
    try
        for scratch in options.scratch
            isdir(scratch) || mkpath(scratch)
        end

        if options.zeros() === nothing
            options.zeros = ()->zeros(eltype(result), size(result))::T
        end

        epmap_journal = journal_init(tasks, options.journal_init_callback; reduce=true)

        epmap_eloop = ElasticLoop(typeof(next_checkpoint(options.id, options.scratch)), tasks, options; isreduce=true)

        tsk_map = @async epmapreduce_map(f, result, epmap_eloop, epmap_journal, options, args...; kwargs...)

        tsk_reduce = @async epmapreduce_reduce!(result, epmap_eloop, epmap_journal, options)

        @debug "waiting for tsk_loop"
        loop(epmap_eloop, epmap_journal, options.journal_task_callback, tsk_map, tsk_reduce)
        @debug "fetching from tsk_reduce"
        result = fetch(tsk_reduce)
        @debug "finished fetching from tsk_reduce"

        journal_final(epmap_journal)
        journal_write(epmap_journal, options.journalfile)
    finally
        for fieldname in fieldnames(SchedulerOptions)
            setfield!(options, fieldname, getfield(options_init, fieldname))
        end
    end
    result, epmap_eloop.tsk_pool_timed_out
end

epmapreduce!(result, f::Function, tasks, args...; kwargs...) = epmapreduce!(result, SchedulerOptions(), f, tasks, args...; kwargs...)

function epmapreduce_fetch_apply(_localresult, ::Type{T}, epmapreduce_fetch, f, itsk, args...; kwargs...) where {T}
    localresult = epmapreduce_fetch(_localresult)::T
    f(localresult, itsk, args...; kwargs...)
    nothing
end

function epmapreduce_map(f, results::T, epmap_eloop, epmap_journal, options, args...; kwargs...) where {T}
    localresults = Dict{Int, Future}()

    checkpoint_orphans = Any[]

    tsk_times = Float64[]
    checkpoint_times = Float64[]
    rm_times = Float64[]

    # work loop
    @sync while true
        @debug "map, iterrupted=$(epmap_eloop.interrupted)"
        epmap_eloop.interrupted && break
        pid = take!(epmap_eloop.pid_channel_map_add)

        @debug "map, pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.
        
        if pid ∈ keys(localresults) # task loop has already run for this pid
            put!(epmap_eloop.pid_channel_map_remove, (pid,false))
            continue
        end

        hostname = ""

        # It is important that this is async in the event that the allocation in options.zeros is large, and takes a significant amount of time.
        # Exceptions will be caught the first time we fetch `localresults[pid]` in the `epmapreduce_fetch_apply` method.
        localresults[pid] = remotecall_default_threadpool(options.zeros, pid)

        epmap_eloop.checkpoints[pid] = nothing

        local preempt_channel_future
        try
            preempt_channel_future = options.preempt_channel_future(pid)
        catch
            @warn "failed to retreive preempt_channel_future.  checkpoint/restart functionality disabled."
            preempt_channel_future = nothing
        end

        @async try
            while true
                if hostname == ""
                    try
                        hostname = remotecall_fetch_timeout(60, 1, 1, nothing, tsk->nothing, tsk->nothing, 0, options.gethostname, pid)
                    catch e
                        @warn "unable to determine hostname for pid=$pid within 60 seconds."
                        logerror(e, Logging.Warn)
                        if epmap_eloop.checkpoints[pid] !== nothing
                            push!(epmap_eloop.reduce_checkpoints, epmap_eloop.checkpoints[pid])
                        end
                        pop!(localresults, pid)
                        pop!(epmap_eloop.checkpoints, pid)
                        put!(epmap_eloop.pid_channel_reduce_remove, (pid,true))
                        @debug "...finished cleanup for hostname failure for pid=$pid."
                        break
                    end
                end

                @debug "map, pid=$pid, interrupted=$(epmap_eloop.interrupted), isempty(epmap_eloop.tsk_pool_todo)=$(isempty(epmap_eloop.tsk_pool_todo))"
                @debug "epmap_eloop.is_reduce_triggered=$(epmap_eloop.is_reduce_triggered)"
                if isempty(epmap_eloop.tsk_pool_todo) || epmap_eloop.interrupted || (epmap_eloop.is_reduce_triggered && !epmap_eloop.checkpoints_are_flushed)
                    if epmap_eloop.checkpoints[pid] !== nothing
                        push!(epmap_eloop.reduce_checkpoints, epmap_eloop.checkpoints[pid])
                    end
                    pop!(localresults, pid)
                    pop!(epmap_eloop.checkpoints, pid)
                    put!(epmap_eloop.pid_channel_map_remove, (pid,false))
                    break
                end

                local tsk
                try
                    tsk = popfirst!(epmap_eloop.tsk_pool_todo)
                catch
                    # just in case another task does popfirst! before us (unlikely)
                    yield()
                    continue
                end

                # compute and reduce
                try
                    options.reporttasks && @info "running task $tsk on process $pid ($hostname); $(nworkers()) workers total; $(length(epmap_eloop.tsk_pool_todo)) tasks left in task-pool."
                    yield()
                    journal_start!(epmap_journal, options.journal_task_callback; stage="tasks", tsk, pid, hostname)
                    remotecall_func_wait_timeout(tsk_times, epmap_eloop, options, preempt_channel_future, options.checkpoint_task, options.restart_task, tsk, epmapreduce_fetch_apply, pid, localresults[pid], T, options.epmapreduce_fetch, f, tsk, args...; kwargs...)
                    journal_stop!(epmap_journal, options.journal_task_callback; stage="tasks", tsk, pid, fault=false)
                    @debug "...pid=$pid ($hostname),tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(epmap_eloop.tsk_pool_todo), tsk_pool_done=$(epmap_eloop.tsk_pool_done) -!"
                catch e
                    @warn "pid=$pid ($hostname), task loop, caught exception during f eval"
                    journal_stop!(epmap_journal, options.journal_task_callback; stage="tasks", tsk, pid, fault=false)
                    if isa(e, TimeoutException) && options.skip_tasks_that_timeout
                        @warn "skipping task '$tsk' that timed out, compute/reduce step"
                        push!(epmap_eloop.tsk_pool_done, tsk)
                        push!(epmap_eloop.tsk_pool_timed_out, tsk)
                    else
                        push!(epmap_eloop.tsk_pool_todo, tsk)
                    end
                    r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, options.maxerrors, options.retries)
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    if r.do_break || r.do_interrupt
                        if epmap_eloop.checkpoints[pid] !== nothing
                            push!(epmap_eloop.reduce_checkpoints, epmap_eloop.checkpoints[pid])
                        end
                        pop!(localresults, pid)
                        pop!(epmap_eloop.checkpoints, pid)
                        put!(epmap_eloop.pid_channel_map_remove, (pid,r.bad_pid))
                        break
                    end
                    continue # no need to checkpoint since the task failed and will be re-run (TODO: or abandoned)
                end

                # checkpoint
                _next_checkpoint = next_checkpoint(options.id, options.scratch)
                try
                    @debug "running checkpoint for task $tsk on process $pid; $(nworkers()) workers total; $(length(epmap_eloop.tsk_pool_todo)) tasks left in task-pool."
                    journal_start!(epmap_journal; stage="checkpoints", tsk, pid, hostname)
                    remotecall_wait_timeout(checkpoint_times, epmap_eloop.tsk_count, options.timeout_multiplier, nothing, tsk->nothing, tsk->nothing, 0, save_checkpoint, pid, options.save_checkpoint, options.epmapreduce_fetch, _next_checkpoint, localresults[pid], T)
                    journal_stop!(epmap_journal; stage="checkpoints", tsk, pid, fault=false)
                    @debug "... checkpoint, pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(epmap_eloop.tsk_pool_todo) -!"
                    push!(epmap_eloop.tsk_pool_done, tsk)
                catch e
                    @warn "pid=$pid ($hostname), checkpoint=$(epmap_eloop.checkpoints[pid]), task loop, caught exception during save_checkpoint"
                    journal_stop!(epmap_journal; stage="checkpoints", tsk, pid, fault=true)
                    @debug "pushing task onto tsk_pool_todo list"
                    if isa(e, TimeoutException) && options.skip_tasks_that_timeout
                        @warn "skipping task '$tsk' that timed out, checkpoint step"
                        push!(epmap_eloop.tsk_pool_done, tsk)
                        push!(epmap_eloop.tsk_pool_timed_out, tsk)
                    else
                        push!(epmap_eloop.tsk_pool_todo, tsk)
                    end
                    @debug "handling exception"
                    r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, options.maxerrors, options.retries)
                    @debug "done handling exception"
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    @debug "caught save checkpoint" r.do_break r.do_interrupt _next_checkpoint
                    # note that `options.rm_checkpoint` should check if the file exists before attempting removal
                    try
                        options.rm_checkpoint(_next_checkpoint)
                    catch e
                        @warn "unable to delete $(_next_checkpoint), manual clean-up may be required"
                        logerror(e, Logging.Warn)
                    end
                    if r.do_break || r.do_interrupt
                        @debug "epmap_eloop.checkpoints[$pid]", epmap_eloop.checkpoints[pid]
                        if epmap_eloop.checkpoints[pid] !== nothing
                            push!(epmap_eloop.reduce_checkpoints, epmap_eloop.checkpoints[pid])
                        end
                        @debug "popping localresults"
                        pop!(localresults, pid)
                        @debug "popping checkpoints"
                        pop!(epmap_eloop.checkpoints, pid)
                        @debug "returning pid to elastic loop"
                        put!(epmap_eloop.pid_channel_map_remove, (pid,r.bad_pid))
                        @debug "done returning pid to elastic loop"
                        break
                    end

                    # populate local reduction with the previous checkpoint since we will re-run the task.
                    if epmap_eloop.checkpoints[pid] === nothing
                        @debug "re-zeroing local result (no existing checkpoints)"
                        localresults[pid] = remotecall_default_threadpool(options.zeros, pid)
                    else
                        @debug "re-setting localresult to the previously saved checkpoint"
                        localresults[pid] = remotecall_default_threadpool(load_checkpoint, pid, options.load_checkpoint, epmap_eloop.checkpoints[pid], T)
                    end

                    continue # there is no new checkpoint, and we need to keep the old checkpoint
                end

                # delete old checkpoint
                old_checkpoint,epmap_eloop.checkpoints[pid] = get(epmap_eloop.checkpoints, pid, nothing),_next_checkpoint
                try
                    if old_checkpoint !== nothing
                        @debug "deleting old checkpoint"
                        journal_start!(epmap_journal; stage="rmcheckpoints", tsk, pid, hostname)
                        options.keepcheckpoints || remotecall_wait_timeout(rm_times, epmap_eloop.tsk_count, options.timeout_multiplier, nothing, tsk->nothing, tsk->nothing, 0, options.rm_checkpoint, pid, old_checkpoint)
                        journal_stop!(epmap_journal; stage="rmcheckpoint", tsk, pid, fault=false)
                    end
                catch e
                    @warn "pid=$pid ($hostname), task loop, caught exception during remove checkpoint, there may be stray check-point files that will be deleted later"
                    push!(checkpoint_orphans, old_checkpoint)
                    journal_stop!(epmap_journal; stage="checkpoints", tsk, pid, fault=true)
                    r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, options.maxerrors, options.retries)
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    if r.do_break || r.do_interrupt
                        if epmap_eloop.checkpoints[pid] !== nothing
                            push!(epmap_eloop.reduce_checkpoints, epmap_eloop.checkpoints[pid])
                        end
                        pop!(localresults, pid)
                        pop!(epmap_eloop.checkpoints, pid)
                        put!(epmap_eloop.pid_channel_map_remove, (pid,r.bad_pid))
                        break
                    end
                end
            end
        catch e
            @warn "map, uncaught exception in worker loop for pid=$pid"
            logerror(e, Logging.Warn)
            @debug "map, putting $pid onto remove channel"
            isopen(epmap_eloop.pid_channel_map_remove) && put!(epmap_eloop.pid_channel_map_remove, (pid,true))
            @debug "map, done putting $pid onto remove channel"
        end
    end
    @debug "exited the map worker loop"
    empty!(epmap_eloop.checkpoints)

    @debug "map, emptied the checkpoints"
    for checkpoint in checkpoint_orphans
        try
            options.rm_checkpoint(checkpoint)
        catch e
            @warn "unable to remove checkpoint: $checkpoint, manual clean-up may be required"
            logerror(e, Logging.Warn)
        end
    end
    @debug "map, deleted the orphaned checkpoints"

    nothing
end

function epmapreduce_reduce!(result::T, epmap_eloop, epmap_journal, options) where {T}
    @debug "entered the reduce method"
    orphans_remove = Set{Any}()

    l = ReentrantLock()

    # Seeding with a large value to account for potential throttling of various cloud storage services
    reduce_times = Float64[300.0]
    rm_times = Float64[300.0]

    @sync while true
        @debug "reduce, interrupted=$(epmap_eloop.interrupted)"
        epmap_eloop.interrupted && break
        pid = take!(epmap_eloop.pid_channel_reduce_add)

        @debug "reduce, pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel when the reduction is done

        hostname = ""
        try
            hostname = remotecall_fetch_timeout(60, 1, 1, nothing, tsk->nothing, tsk->nothing, 0, options.gethostname, pid)
        catch e
            @warn "unable to determine host name for pid=$pid"
            logerror(e, Logging.Warn)
            put!(epmap_eloop.pid_channel_reduce_remove, (pid,true))
            continue
        end

        @async try
            while true
                @debug "reduce, pid=$pid, epmap_eloop.interrupted=$(epmap_eloop.interrupted)"
                epmap_eloop.interrupted && break

                epmap_eloop.reduce_checkpoints_is_dirty[pid] = true

                @debug "reduce, waiting for lock on pid=$pid"
                lock(l)
                @debug "reduce, got lock on pid=$pid"

                n_checkpoints = epmap_eloop.is_reduce_triggered ? length(epmap_eloop.reduce_checkpoints_snapshot) : length(epmap_eloop.reduce_checkpoints)

                @debug "reduce, pid=$pid, at exit condition, n_checkpoints=$n_checkpoints"
                if n_checkpoints < 2
                    unlock(l)
                    @debug "reduce, popping pid=$pid from dirty list"
                    pop!(epmap_eloop.reduce_checkpoints_is_dirty, pid)
                    @debug "reduce, putting $pid onto reduce remove channel"
                    put!(epmap_eloop.pid_channel_reduce_remove, (pid,false))
                    @debug "reduce, done putting $pid onto reduce remove channel"
                    break
                end

                # reduce two checkpoints into a third checkpoint
                if n_checkpoints > 1
                    @info "reducing from $n_checkpoints $(epmap_eloop.is_reduce_triggered ? "snapshot " : "")checkpoints using process $pid ($(nworkers()) workers, $(length(epmap_eloop.used_pids_reduce)) reduce workers)."
                    local checkpoint1
                    try
                        @debug "reduce, popping first checkpoint, pid=$pid"
                        if epmap_eloop.is_reduce_triggered
                            checkpoint1 = popfirst!(epmap_eloop.reduce_checkpoints_snapshot)
                            icheckpoint1 = findfirst(checkpoint->checkpoint == checkpoint1, epmap_eloop.reduce_checkpoints)
                            icheckpoint1 === nothing || deleteat!(epmap_eloop.reduce_checkpoints, icheckpoint1)
                        else
                            checkpoint1 = popfirst!(epmap_eloop.reduce_checkpoints)
                        end
                        @debug "reduce, popped first checkpoint, pid=$pid" checkpoint1
                    catch e
                        @warn "reduce, unable to pop checkpoint 1"
                        logerror(e, Logging.Warn)
                        epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                        unlock(l)
                        @debug "reduce, released lock on pid=$pid"
                        continue
                    end

                    local checkpoint2
                    try
                        @debug "reduce, popping second checkpoint, pid=$pid"
                        if epmap_eloop.is_reduce_triggered
                            checkpoint2 = popfirst!(epmap_eloop.reduce_checkpoints_snapshot)
                            icheckpoint2 = findfirst(checkpoint->checkpoint == checkpoint2, epmap_eloop.reduce_checkpoints)
                            icheckpoint2 === nothing || deleteat!(epmap_eloop.reduce_checkpoints, icheckpoint2)
                        else
                            checkpoint2 = popfirst!(epmap_eloop.reduce_checkpoints)
                        end
                        @debug "reduce, popped second checkpoint, pid=$pid" checkpoint2
                    catch e
                        @warn "reduce, unable to pop checkpoint 2"
                        logerror(e, Logging.Warn)
                        push!(epmap_eloop.reduce_checkpoints, checkpoint1)
                        epmap_eloop.is_reduce_triggered && push!(epmap_eloop.reduce_checkpoints_snapshot, checkpoint1)
                        epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                        unlock(l)
                        @debug "reduce, released lock on pid=$pid"
                        continue
                    end

                    local checkpoint3
                    try
                        @debug "reduce, make third checkpoint, pid=$pid"
                        checkpoint3 = next_checkpoint(options.id, options.scratch)
                        @debug "reduce, made third checkpoint, pid=$pid" checkpoint3
                    catch e
                        @warn "reduce, unable to create checkpoint 3"
                        logerror(e, Logging.Warn)
                        push!(epmap_eloop.reduce_checkpoints, checkpoint1, checkpoint2)
                        epmap_eloop.is_reduce_triggered && push!(epmap_eloop.reduce_checkpoints_snapshot, checkpoint1, checkpoint2)
                        epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                        unlock(l)
                        @debug "reduce, released lock on pid=$pid"
                        continue
                    end
                else
                    epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                    unlock(l)
                    @debug "reduce, no checkpoints to reduce, released lock on pid=$pid"
                    continue
                end
                unlock(l)
                @debug "reduce, released lock on pid=$pid"

                try
                    @debug "reducing into checkpoint3, pid=$pid" checkpoint3
                    journal_start!(epmap_journal; stage="reduce", tsk=0, pid, hostname)
                    # We don't have a good way for estimating the number of reduction tasks (due to the dynamic nature of the resources), so we choose an arbibrary number (10).
                    remotecall_wait_timeout(reduce_times, 10, options.timeout_multiplier, nothing, tsk->nothing, tsk->nothing, 0, reduce, pid, options.reducer!, options.save_checkpoint, options.epmapreduce_fetch, options.load_checkpoint, checkpoint1, checkpoint2, checkpoint3, T)
                    journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=false)
                    push!(epmap_eloop.reduce_checkpoints, checkpoint3)
                    epmap_eloop.is_reduce_triggered && push!(epmap_eloop.reduce_checkpoints_snapshot, checkpoint3)
                    @debug "pushed reduced checkpoint3, pid=$pid" checkpoint3
                catch e
                    push!(epmap_eloop.reduce_checkpoints, checkpoint1, checkpoint2)
                    epmap_eloop.is_reduce_triggered && push!(epmap_eloop.reduce_checkpoints_snapshot, checkpoint1, checkpoint2)
                    @warn "pid=$pid ($hostname), reduce loop, caught exception during reduce"
                    r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, options.maxerrors, options.retries)
                    journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    if r.do_break || r.do_interrupt
                        pop!(epmap_eloop.reduce_checkpoints_is_dirty, pid)
                        put!(epmap_eloop.pid_channel_reduce_remove, (pid,r.bad_pid))
                        epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                        break
                    end
                    epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                    continue
                end

                # remove checkpoint 1
                try
                    options.keepcheckpoints || @debug "removing checkpoint 1, pid=$pid" checkpoint1
                    journal_start!(epmap_journal; stage="reduce", tsk=0, pid, hostname)
                    # We don't have a good way for estimating the number of deletion tasks (due to the dynamic nature of the resources), so we choose an arbibrary number (10).
                    options.keepcheckpoints || remotecall_wait_timeout(rm_times, 10, options.timeout_multiplier, nothing, tsk->nothing, tsk->nothing,  0, options.rm_checkpoint, pid, checkpoint1)
                    journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=false)
                    options.keepcheckpoints || @debug "removed checkpoint 1, pid=$pid" checkpoint1
                catch e
                    @warn "pid=$pid ($hostname), reduce loop, caught exception during remove checkpoint 1"
                    r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, options.maxerrors, options.retries)
                    journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                    push!(orphans_remove, checkpoint1, checkpoint2)
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    if r.do_break || r.do_interrupt
                        pop!(epmap_eloop.reduce_checkpoints_is_dirty, pid)
                        put!(epmap_eloop.pid_channel_reduce_remove, (pid,r.bad_pid))
                        break
                    end
                    epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                    continue
                end

                # remove checkpoint 2
                try
                    options.keepcheckpoints || @debug "removing checkpoint 2, pid=$pid" checkpoint2
                    journal_start!(epmap_journal; stage="reduce", tsk=0, pid, hostname)
                    # We don't have a good way for estimating the number of deletion tasks (due to the dynamic nature of the resources), so we choose an arbibrary number (10).
                    options.keepcheckpoints || remotecall_wait_timeout(rm_times, 10, options.timeout_multiplier, nothing, tsk->nothing, tsk->nothing, 0, options.rm_checkpoint, pid, checkpoint2)
                    journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=false)
                    options.keepcheckpoints || @debug "removed checkpoint 2, pid=$pid" checkpoint2
                catch e
                    @warn "pid=$pid ($hostname), reduce loop, caught exception during remove checkpoint 2"
                    r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, options.maxerrors, options.retries)
                    journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                    push!(orphans_remove, checkpoint2)
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    if r.do_break || r.do_interrupt
                        pop!(epmap_eloop.reduce_checkpoints_is_dirty, pid)
                        put!(epmap_eloop.pid_channel_reduce_remove, (pid,r.bad_pid))
                        break
                    end
                end

                epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
            end
        catch e
            @warn "reduce, uncaught exception in worker loop for pid=$pid"
            logerror(e, Logging.Warn)
            @debug "reduce, putting $pid onto remove channel"
            isopen(epmap_eloop.pid_channel_reduce_remove) && put!(epmap_eloop.pid_channel_reduce_remove, (pid,true))
            @debug "reduce, done putting $pid onto remove channel"
        end
    end
    @debug "reduce, exited the reduce worker loop"

    length(epmap_eloop.reduce_checkpoints) == 0 && @warn "there are no checkpoints to reduce indicating that no tasks were run"

    for i in 1:10
        try
            if length(epmap_eloop.reduce_checkpoints) > 0
                options.reducer!(result, load_checkpoint(options.load_checkpoint, epmap_eloop.reduce_checkpoints[1], T))
            end
            break
        catch e
            s = min(2.0^(i-1), 60.0) + rand()
            @warn "failed to reduce from checkpoint on master, retry ($i of 10) in $s seconds, epmap_eloop.reduce_checkpoints=$(epmap_eloop.reduce_checkpoints)"
            logerror(e, Logging.Warn)
            if i == 10 || !isfile(epmap_eloop.reduce_checkpoints[1])
                throw(e)
            end
            sleep(s)
        end
    end
    @debug "reduce, finished final reduction on master"

    @debug "deleting orphaned checkpoints"
    if !(options.keepcheckpoints)
        for checkpoint in epmap_eloop.reduce_checkpoints
            try
                options.rm_checkpoint(checkpoint)
            catch
                @warn "unable to remove final checkpoint $checkpoint"
            end
        end
        for checkpoint in orphans_remove
            try
                options.rm_checkpoint(checkpoint)
            catch
                @warn "unable to remove orphan checkpoint: $checkpoint"
            end
        end
    end
    @debug "reduce, finished final clean-up on master"

    result
end

let CID::Int = 1
    global next_checkpoint_id
    next_checkpoint_id() = (id = CID; CID += 1; id)
end
let SID::Int = 1
    global next_scratch_index
    next_scratch_index(n) = (id = clamp(SID, 1, n); SID = id == n ? 1 : id+1; id)
end
function next_checkpoint(id, scratch)
    joinpath(scratch[next_scratch_index(length(scratch))], string("checkpoint-", id, "-", next_checkpoint_id()))
end

function reduce(reducer!, save_checkpoint_method, fetch_method, load_checkpoint_method, checkpoint1, checkpoint2, checkpoint3, ::Type{T}) where {T}
    @debug "reduce, load checkpoint 1"
    c1 = load_checkpoint(load_checkpoint_method, checkpoint1, T)
    @debug "reduce, load checkpoint 2"
    c2 = load_checkpoint(load_checkpoint_method, checkpoint2, T)
    @debug "reduce, reducer"
    reducer!(c2, c1)
    @debug "reduce, serialize"
    save_checkpoint(save_checkpoint_method, fetch_method, checkpoint3, c2, T)
    @debug "reduce, done"
    nothing
end

save_checkpoint(save_checkpoint_method, fetch_method, checkpoint, _localresult, ::Type{T}) where {T} = (save_checkpoint_method(checkpoint, fetch_method(_localresult)::T); nothing)
load_checkpoint(load_checkpoint_method, checkpoint, ::Type{T}) where {T} = load_checkpoint_method(checkpoint)::T

default_save_checkpoint(checkpoint, localresult) = serialize(checkpoint, localresult)
default_load_checkpoint(checkpoint) = deserialize(checkpoint)

default_rm_checkpoint(checkpoint) = isfile(checkpoint) && rm(checkpoint)

include("epmap_collect.jl")

export SchedulerOptions, epmap, epmapreduce!, trigger_reduction!, total_tasks, pending_tasks, complete_tasks

end
