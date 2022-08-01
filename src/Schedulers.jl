module Schedulers

using Dates, Distributed, DistributedOperations, JSON, Logging, Printf, Random, Serialization, Statistics

epmap_default_addprocs = n->addprocs(n)
epmap_default_preempted = ()->false
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

function check_for_preempted(pid, epmap_preempted)
    preempted = false
    try
        if remotecall_fetch(epmap_preempted, pid)
            preempted = true
        end
    catch e
        @debug "unable to call preempted method"
    end
    preempted
end

function journal_init(tsks, epmap_journal_init_callback; reduce)
    journal = reduce ? Dict{String,Any}("tasks"=>Dict(), "pids"=>Dict()) : Dict{String,Any}("tasks"=>Dict())
    journal["start"] = now_formatted()
    for tsk in tsks
        journal["tasks"][tsk] = Dict("id"=>"$tsk", "trials"=>Dict[])
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
        write(filename, json(journal))
    end
end

function journal_start!(journal, epmap_journal_task_callback=tsk->nothing; stage, tsk, pid, hostname)
    if stage ∈ ("task", "checkpoint")
        push!(journal["tasks"][tsk]["trials"],
            Dict(
                "pid" => pid,
                "hostname" => hostname,
                "task" => Dict(),
                "checkpoint" => Dict()
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

    if stage ∈ ("task", "checkpoint")
        journal["tasks"][tsk]["trials"][end][stage]["start"] = now_formatted()
    elseif stage ∈ ("restart", "reduce")
        journal["pids"][pid]["tic"] = time()
    end

    if stage == "task"
        epmap_journal_task_callback(journal["tasks"][tsk])
    end
end

function journal_stop!(journal, epmap_journal_task_callback=tsk->nothing; stage, tsk, pid, fault)
    if stage ∈ ("task", "checkpoint")
        journal["tasks"][tsk]["trials"][end][stage]["status"] = fault ? "failed" : "succeeded"
        journal["tasks"][tsk]["trials"][end][stage]["stop"] = now_formatted()
    elseif stage ∈ ("restart", "reduce")
        journal["pids"][pid][stage]["elapsed"] += time() - journal["pids"][pid]["tic"]
        if fault
            journal["pids"][pid][stage]["faults"] += 1
        end
    end

    if stage == "task"
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
        catch
            @debug "caught error in load_modules_on_new_workers"
        end
    end
    nothing
end

function load_functions_on_new_workers(pid)
    _names = names(Main; imported=true)
    for _name in _names
        try
            @sync if isa(Base.eval(Main, _name), Function)
                @async remotecall_fetch(Base.eval, pid, Main, :(function $_name end))
            end
            @sync for m in Base.eval(Main, :(methods($_name)))
                @async remotecall_fetch(Base.eval, pid, Main, :($m))
            end
        catch
            if _name ∉ (Symbol("@enter"), Symbol("@run"), :ans, :vscodedisplay)
                @debug "caught error in load_functions_on_new_workers for function $_name"
            end
        end
    end
end

# for performance metrics, track when the pid is started
const _pid_up_timestamp = Dict{Int, Float64}()

mutable struct ElasticLoop{FAddProcs<:Function,FInit<:Function,FMinWorkers<:Function,FMaxWorkers<:Function,FNWorkers<:Function,FQuantum<:Function,T,C}
    epmap_use_master::Bool
    initialized_pids::Set{Int}
    used_pids::Set{Int}
    pid_channel_map_add::Channel{Int}
    pid_channel_map_remove::Channel{Tuple{Int,Bool}}
    pid_channel_reduce_add::Channel{Int}
    pid_channel_reduce_remove::Channel{Tuple{Int,Bool}}
    epmap_addprocs::FAddProcs
    epmap_init::FInit
    epmap_minworkers::FMinWorkers
    epmap_maxworkers::FMaxWorkers
    epmap_quantum::FQuantum
    epmap_nworkers::FNWorkers
    tsk_pool_todo::Vector{T}
    tsk_pool_done::Vector{T}
    tsk_count::Int
    reduce_checkpoints::Vector{C}
    reduce_checkpoints_is_dirty::Dict{Int,Bool}
    checkpoints::Dict{Int,Union{C,Nothing}}
    interrupted::Bool
    errored::Bool
    pid_failures::Dict{Int,Int}
end
function ElasticLoop(::Type{C};
        epmap_init,
        epmap_addprocs,
        epmap_quantum,
        epmap_minworkers,
        epmap_maxworkers,
        epmap_nworkers,
        epmap_usemaster,
        tasks,
        isreduce) where {C}
    _tsk_pool_todo = vec(collect(tasks))

    eloop = ElasticLoop(
        epmap_usemaster,
        epmap_usemaster ? Set{Int}() : Set(1),
        epmap_usemaster ? Set{Int}() : Set(1),
        Channel{Int}(32),
        Channel{Tuple{Int,Bool}}(32),
        Channel{Int}(32),
        Channel{Tuple{Int,Bool}}(32),
        epmap_addprocs,
        epmap_init,
        isa(epmap_minworkers, Function) ? epmap_minworkers : ()->epmap_minworkers,
        isa(epmap_maxworkers, Function) ? epmap_maxworkers : ()->epmap_maxworkers,
        isa(epmap_quantum, Function) ? epmap_quantum : ()->epmap_quantum,
        epmap_nworkers,
        _tsk_pool_todo,
        empty(_tsk_pool_todo),
        length(_tsk_pool_todo),
        C[],
        Dict{Int,Bool}(),
        Dict{Int,Union{Nothing,C}}(),
        false,
        false,
        Dict{Int,Int}())

    if !isreduce
        close(eloop.pid_channel_reduce_add)
        close(eloop.pid_channel_reduce_remove)
    end

    eloop
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

function loop(eloop::ElasticLoop)
    polling_interval = parse(Int, get(ENV, "SCHEDULERS_POLLING_INTERVAL", "1"))
    init_tasks = Dict{Int,Task}()

    bad_pids = Set{Int}()
    wrkrs = Dict{Int, Union{Distributed.LocalProcess, Distributed.Worker}}()

    tsk_addrmprocs = @async nothing

    is_tasks_done_message_sent = false
    is_reduce_done_message_sent = false

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
        
        @debug "check for complete reduction when tasks are done ($is_tasks_done), and there are no more checkpoints: ($(length(eloop.reduce_checkpoints)) pending, $(length(eloop.checkpoints)) active)"
        if is_tasks_done && !is_reduce_active
            if isopen(eloop.pid_channel_reduce_add) && !is_reduce_done_message_sent
                put!(eloop.pid_channel_reduce_add, -1)
                is_reduce_done_message_sent = true
            end
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

        new_pids = filter(pid->(pid ∉ eloop.used_pids && pid ∉ bad_pids), workers())

        @debug "workers()=$(workers()), new_pids=$new_pids, used_pids=$(eloop.used_pids), bad_pids=$bad_pids"
        yield()

        for new_pid in new_pids
            push!(eloop.used_pids, new_pid)
            init_tasks[new_pid] = @async begin
                try
                    if new_pid ∉ eloop.initialized_pids
                        @debug "initializing failure count on $new_pid"
                        yield()
                        eloop.pid_failures[new_pid] = 0
                        @debug "loading modules on $new_pid"
                        yield()
                        load_modules_on_new_workers(new_pid)
                        @debug "loading functions on $new_pid"
                        yield()
                        load_functions_on_new_workers(new_pid)
                        @debug "calling init on new $new_pid"
                        yield()
                        eloop.epmap_init(new_pid)
                        @debug "done loading functions modules, and calling init on $new_pid"
                        yield()
                        _pid_up_timestamp[new_pid] = time()
                        push!(eloop.initialized_pids, new_pid)
                    end

                    if !haskey(Distributed.map_pid_wrkr, new_pid)
                        @warn "worker with pid=$new_pid is not registered"
                        new_pid ∈ keys(wrkrs) && pop!(wrkrs, new_pid)
                        new_pid ∈ eloop.used_pids && pop!(eloop.used_pids, new_pid)
                    else
                        wrkrs[new_pid] = Distributed.map_pid_wrkr[new_pid]
                        if is_more_tasks
                            @debug "putting pid=$new_pid onto map channel"
                            put!(eloop.pid_channel_map_add, new_pid)
                        elseif is_more_checkpoints
                            @debug "putting pid=$new_pid onto reduce channel"
                            put!(eloop.pid_channel_reduce_add, new_pid)
                        else
                            new_pid ∈ eloop.used_pids && pop!(eloop.used_pids, new_pid)
                        end
                    end
                catch e
                    @warn "problem initializing $new_pid, removing $new_pid from cluster."
                    logerror(e, Logging.Warn)
                    push!(bad_pids, new_pid)
                    new_pid ∈ eloop.used_pids && pop!(eloop.used_pids, new_pid)
                end
            end
        end

        # check to see if we need to add or remove machines
        δ,n_remaining_tasks = 0,0
        try
            n_remaining_tasks = length(eloop.tsk_pool_todo) + max(length(eloop.reduce_checkpoints) - 1, 0)
            δ = min(_epmap_maxworkers - _epmap_nworkers, _epmap_quantum, n_remaining_tasks - _epmap_nworkers)
            # note that 1. we will only remove a machine if it is not in the 'eloop.used_pids' vector.
            # and 2. we will only remove machines if we are left with at least epmap_minworkers.
            if _epmap_nworkers + δ < _epmap_minworkers
                δ = _epmap_minworkers - _epmap_nworkers
            end
        catch e
            @warn "problem in Schedulers.jl elastic loop when computing the number of new machines to add"
            logerror(e, Logging.Warn)
        end

        @debug "add at most $_epmap_quantum machines when there are less than $_epmap_maxworkers, and there are less then the current task count: $n_remaining_tasks (δ=$δ, n=$_epmap_nworkers)"
        @debug "remove machine when there are more than $_epmap_minworkers, and less tasks ($n_remaining_tasks) than workers ($_epmap_nworkers), and when those workers are free (currently there are $_epmap_nworkers workers, and $(length(eloop.used_pids)) busy procs inclusive of process 1)"
        if istaskdone(tsk_addrmprocs)
            tsk_addrmprocs = @async begin
                if !isempty(bad_pids)
                    @debug "removing bad pids: $bad_pids"
                    while !isempty(bad_pids)
                        bad_pid = pop!(bad_pids)
                        if haskey(Distributed.map_pid_wrkr, bad_pid)
                            @debug "bad pid=$bad_pid is know to Distributed, calling rmprocs"
                            rmprocs(bad_pid)
                        else
                            @debug "bad pid=$bad_pid is not known to Distributed, calling kill"
                            try
                                # required for cluster managers that require clean-up when the julia process on a worker dies:
                                Distributed.kill(wrkrs[bad_pid].manager, bad_pid, wrkrs[bad_pid].config)
                            catch e
                                @warn "unable to kill bad worker with pid=$bad_pid"
                                logerror(e, Logging.Warn)
                            end
                        end
                        bad_pid ∈ wrkrs && delete!(wrkrs, bad_pid)
                    end
                elseif δ > 0
                    try
                        @debug "adding $δ procs"
                        eloop.epmap_addprocs(δ)
                    catch e
                        @error "problem adding new processes"
                        logerror(e, Logging.Error)
                    end
                elseif δ < 0
                    @debug "determining free pids"
                    freepids = filter(pid->pid ∉ eloop.used_pids, workers())
                    @debug "done determining free pids"

                    @debug "push onto rmpids"
                    rmpids = freepids[1:min(-δ, length(freepids))]
                    @debug "done push onto rmpids"

                    try
                        @debug "removing $rmpids from $freepids"
                        rmprocs(rmpids)
                    catch e
                        @error "problem removing old processes"
                        logerror(e, Logging.Error)
                    end
                end
                @debug "done making new async add/rmprocs"
            end
        else
            @debug "previous addprocs/rmprocs is not complete, expect delays."
        end

        @debug "checking for workers sent from the map"
        yield()
        try
            while isready(eloop.pid_channel_map_remove)
                pid,isbad = take!(eloop.pid_channel_map_remove)
                @debug "map channel, received pid=$pid, making sure that it is initialized"
                yield()
                wait(init_tasks[pid])
                isbad && push!(bad_pids, pid)
                @debug "map channel, $pid is initialied, removing from used_pids"
                pid ∈ eloop.used_pids && pop!(eloop.used_pids, pid)
                @debug "map channel, done removing $pid from used_pids, used_pids=$(eloop.used_pids)"
            end
        catch e
            @warn "problem in Schedulers.jl elastic loop when removing workers from map"
            logerror(e)
        end

        @debug "checking for workers sent from the reduce"
        yield()
        try
            while isready(eloop.pid_channel_reduce_remove)
                pid,isbad = take!(eloop.pid_channel_reduce_remove)
                @debug "reduce channel, received pid=$pid (isbad=$isbad), making sure that it is initialized"
                yield()
                wait(init_tasks[pid])
                isbad && push!(bad_pids, pid)
                @debug "reduce_channel, $pid is initialied, removing from used_pids"
                pid ∈ eloop.used_pids && pop!(eloop.used_pids, pid)
                @debug "reduce channel, done removing $pid from used_pids, used_pids=$(eloop.used_pids)"
            end
        catch e
            @warn "problem in Schedulers.jl elastic loop when removing workers from reduce"
            logerror(e)
        end

        @debug "sleeping for $polling_interval seconds"
        sleep(polling_interval)
    end
    @debug "exited the elastic loop"

    @debug "ensure that tsk_addrmprocs is done."
    tic = time()
    while !istaskdone(tsk_addrmprocs)
        if time() - tic > 10
            @warn "delayed remove/add procs tasks took longer than 10 seconds, giving up..."
            # try
            #     Base.throwto(tsk_addrmprocs, ErrorException(""))
            # catch
            # end
            break
        end
        sleep(1)
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
            rmprocs(workers()[1:n])
            @debug "done trimming $n workers"
        end
    catch e
        @warn "problem trimming workers after map-reduce"
        logerror(e, Logging.Warn)
    end
    @debug "elastic loop, done trimming workers"

    nothing
end

"""
    epmap(f, tasks, args...; pmap_kwargs..., f_kwargs...)

where `f` is the map function, and `tasks` is an iterable collection of tasks.  The function `f`
takes the positional arguments `args`, and the keyword arguments `f_args`.  The optional arguments
`pmap_kwargs` are as follows.

## pmap_kwargs
* `epmap_retries=0` number of times to retry a task on a given machine before removing that machine from the cluster
* `epmap_maxerrors=Inf` the maximum number of errors before we give-up and exit
* `epmap_minworkers=nworkers` method giving the minimum number of workers to elastically shrink to
* `epmap_maxworkers=nworkers` method giving the maximum number of workers to elastically expand to
* `epmap_usemaster=false` assign tasks to the master process?
* `epmap_nworkers=nworkers` the number of machines currently provisioned for work[1]
* `epmap_quantum=()->32` the maximum number of workers to elastically add at a time
* `epmap_addprocs=n->addprocs(n)` method for adding n processes (will depend on the cluster manager being used)
* `epmap_init=pid->nothing` after starting a worker, this method is run on that worker.
* `epmap_preempted=()->false` method for determining of a machine got pre-empted (removed on purpose)[2]
* `epmap_reporttasks=true` log task assignment
* `epmap_journal_init_callback=tsks->nothing` additional method when intializing the journal
* `epmap_journal_task_callback=tsk->nothing` additional method when journaling a task

## Notes
[1] The number of machines provisioined may be greater than the number of workers in the cluster since with
some cluster managers, there may be a delay between the provisioining of a machine, and when it is added to the
Julia cluster.
[2] For example, on Azure Cloud a SPOT instance will be pre-empted if someone is willing to pay more for it
"""
function epmap(f::Function, tasks, args...;
        epmap_retries = 0,
        epmap_maxerrors = Inf,
        epmap_minworkers = nworkers,
        epmap_maxworkers = nworkers,
        epmap_usemaster = false,
        epmap_nworkers = nworkers,
        epmap_quantum = ()->32,
        epmap_addprocs = epmap_default_addprocs,
        epmap_init = epmap_default_init,
        epmap_preempted = epmap_default_preempted,
        epmap_reporttasks = true,
        epmap_journal_init_callback = tsks->nothing,
        epmap_journal_task_callback = tsk->nothing,
        kwargs...)
    eloop = ElasticLoop(Nothing;
        epmap_init,
        epmap_addprocs,
        epmap_quantum,
        epmap_minworkers,
        epmap_maxworkers,
        epmap_nworkers,
        epmap_usemaster,
        tasks,
        isreduce = false)

    tsk_eloop = @async loop(eloop)

    journal = journal_init(tasks, epmap_journal_init_callback; reduce=false)

    # work loop
    @sync while true
        eloop.interrupted && break
        pid = take!(eloop.pid_channel_map_add)

        @debug "pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.

        local hostname
        try
            hostname = remotecall_fetch(gethostname, pid)
        catch e
            @warn "unable to determine hostname for pid=$pid"
            logerror(e, Logging.Warn)
            put!(eloop.pid_channel_map_remove, (pid,true))
            continue
        end

        @async while true
            is_preempted = check_for_preempted(pid, epmap_preempted)
            if is_preempted || length(eloop.tsk_pool_done) == eloop.tsk_count || eloop.interrupted
                @debug "putting $pid onto map_remove channel"
                put!(eloop.pid_channel_map_remove, (pid,is_preempted))
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
                epmap_reporttasks && @info "running task $tsk on process $pid ($hostname); $(nworkers()) workers total; $(length(eloop.tsk_pool_todo)) tasks left in task-pool."
                yield()
                journal_start!(journal, epmap_journal_task_callback; stage="task", tsk, pid, hostname)
                remotecall_fetch(f, pid, tsk, args...; kwargs...)
                journal_stop!(journal, epmap_journal_task_callback; stage="task", tsk, pid, fault=false)
                push!(eloop.tsk_pool_done, tsk)
                @debug "...pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(eloop.tsk_pool_todo), tsk_pool_done=$(eloop.tsk_pool_done) -!"
                yield()
            catch e
                @warn "caught an exception, there have been $(eloop.pid_failures[pid]) failure(s) on process $pid ($hostname)..."
                journal_stop!(journal, epmap_journal_task_callback; stage="task", tsk, pid, fault=true)
                push!(eloop.tsk_pool_todo, tsk)
                r = handle_exception(e, pid, hostname, eloop.pid_failures, epmap_maxerrors, epmap_retries)
                if r.do_break || r.do_interrupt
                    put!(eloop.pid_channel_map_remove, (pid,r.bad_pid))
                end
                eloop.interrupted = r.do_interrupt
                eloop.errored = r.do_error
                r.do_break && break
            end
        end
    end
    @debug "exiting the map loop"
    fetch(tsk_eloop)

    journal_final(journal)

    journal
end

default_reducer!(x, y) = (x .+= y; nothing)
"""
    epmapreduce!(result, f, tasks, args...; epmap_kwargs..., kwargs...) -> result

where f is the map function, and `tasks` are an iterable set of tasks to map over.  The
positional arguments `args` and the named arguments `kwargs` are passed to `f` which has
the method signature: `f(localresult, f, task, args; kwargs...)`.  `localresult` is
the assoicated partial reduction contribution to `result`.

## epmap_kwargs
* `epmap_reducer! = default_reducer!` the method used to reduce the result. The default is `(x,y)->(x .+= y)`
* `epmap_zeros = ()->zeros(eltype(result), size(result))` the method used to initiaize partial reductions
* `epmap_retries=0` number of times to retry a task on a given machine before removing that machine from the cluster
* `epmap_maxerrors=Inf` the maximum number of errors before we give-up and exit
* `epmap_minworkers=nworkers` method giving the minimum number of workers to elastically shrink to
* `epmap_maxworkers=nworkers` method giving the maximum number of workers to elastically expand to
* `epmap_usemaster=false` assign tasks to the master process?
* `epmap_nworkers=nworkers` the number of machines currently provisioned for work[1]
* `epmap_quantum=()->32` the maximum number of workers to elastically add at a time
* `epmap_addprocs=n->addprocs(n)` method for adding n processes (will depend on the cluster manager being used)
* `epmap_init=pid->nothing` after starting a worker, this method is run on that worker.
* `epmap_scratch=["/scratch"]` storage location accesible to all cluster machines (e.g NFS, Azure blobstore,...)[3]
* `epmap_reporttasks=true` log task assignment
* `epmap_journalfile=""` write a journal showing what was computed where to a json file
* `epmap_journal_init_callback=tsks->nothing` additional method when intializing the journal
* `epmap_journal_task_callback=tsk->nothing` additional method when journaling a task

## Notes
[1] The number of machines provisioined may be greater than the number of workers in the cluster since with
some cluster managers, there may be a delay between the provisioining of a machine, and when it is added to the
Julia cluster.
[2] For example, on Azure Cloud a SPOT instance will be pre-empted if someone is willing to pay more for it
[3] If more than one scratch location is selected, then check-point files will be distributed across those locations.
This can be useful if you are, for example, constrained by cloud storage through-put limits.

# Examples
## Example 1
With the assumption that `/scratch` is accesible from all workers:
```
using Distributed
addprocs(2)
@everywhere using Distributed, Schedulers
@everywhere f(x, tsk) = x .+= tsk; nothing)
result = epmapreduce!(zeros(Float32,10), f, 1:100)
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
@everywhere f(x, tsk) = x .+= tsk; nothing)
result = epmapreduce!(zeros(Float32,10), f, 1:100; epmap_scratch=container)
rmprocs(workers())
```
"""
function epmapreduce!(result::T, f, tasks, args...;
        epmap_reducer! = default_reducer!,
        epmap_zeros = nothing,
        epmap_save_checkpoint = default_save_checkpoint,
        epmap_rm_checkpoint = default_rm_checkpoint,
        epmapreduce_id = randstring(6),
        epmap_minworkers = nworkers,
        epmap_maxworkers = nworkers,
        epmap_usemaster = false,
        epmap_nworkers = nworkers,
        epmap_quantum = ()->32,
        epmap_addprocs = epmap_default_addprocs,
        epmap_init = epmap_default_init,
        epmap_preempted = epmap_default_preempted,
        epmap_scratch = ["/scratch"],
        epmap_reporttasks = true,
        epmap_maxerrors = Inf,
        epmap_retries = 0,
        epmap_journalfile = "",
        epmap_keepcheckpoints = false, # used for unit testing / debugging
        epmap_journal_init_callback=tsks->nothing, # additional method when intializing the journal
        epmap_journal_task_callback=tsk->nothing, # additional method when journaling a task
        kwargs...) where {T,N}
    epmap_scratches = isa(epmap_scratch, AbstractArray) ? epmap_scratch : [epmap_scratch]
    for scratch in epmap_scratches
        isdir(scratch) || mkpath(scratch)
    end
    if epmap_zeros === nothing
        epmap_zeros = ()->zeros(eltype(result), size(result))::T
    end

    epmap_journal = journal_init(tasks, epmap_journal_init_callback; reduce=true)

    epmap_eloop = ElasticLoop(typeof(next_checkpoint(epmapreduce_id, epmap_scratches));
        epmap_init,
        epmap_addprocs,
        epmap_quantum,
        epmap_minworkers,
        epmap_maxworkers,
        epmap_nworkers,
        epmap_usemaster,
        tasks,
        isreduce = true)

    tsk_loop = @async loop(epmap_eloop)

    tsk_map = @async epmapreduce_map(f, result, epmap_eloop, epmap_journal, args...;
        epmap_save_checkpoint,
        epmap_rm_checkpoint,
        epmapreduce_id,
        epmap_zeros,
        epmap_preempted,
        epmap_scratches,
        epmap_reporttasks,
        epmap_maxerrors,
        epmap_retries,
        epmap_keepcheckpoints,
        epmap_journal_task_callback,
        kwargs...)

    tsk_reduce = @async epmapreduce_reduce!(result, epmap_eloop, epmap_journal;
        epmapreduce_id,
        epmap_reducer!,
        epmap_preempted,
        epmap_scratches,
        epmap_maxerrors,
        epmap_retries,
        epmap_rm_checkpoint,
        epmap_keepcheckpoints)

    @debug "waiting for tsk_map"
    wait(tsk_map)
    @debug "waiting for tsk_loop"
    wait(tsk_loop)
    @debug "waiting for tsk_reduce"
    result = fetch(tsk_reduce)
    @debug "finished waiting for tsk_reduce"

    journal_final(epmap_journal)
    journal_write(epmap_journal, epmap_journalfile)

    result
end

function epmapreduce_fetch_apply(_localresult, ::Type{T}, f, itsk, args...; kwargs...) where {T}
    localresult = fetch(_localresult)::T
    f(localresult, itsk, args...; kwargs...)
    nothing
end

function epmapreduce_map(f, results::T, epmap_eloop, epmap_journal, args...;
        epmap_save_checkpoint,
        epmap_rm_checkpoint,
        epmapreduce_id,
        epmap_zeros,
        epmap_preempted,
        epmap_scratches,
        epmap_reporttasks,
        epmap_maxerrors,
        epmap_retries,
        epmap_keepcheckpoints,
        epmap_journal_task_callback,
        kwargs...) where {T}
    localresults = Dict{Int, Future}()

    checkpoint_orphans = Any[]

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

        local hostname
        try
            hostname = remotecall_fetch(gethostname, pid)
        catch e
            @warn "unable to determine hostname for pid=$pid"
            logerror(e, Logging.Warn)
            haskey(localresults, pid) && deleteat!(localresults, pid)
            put!(epmap_eloop.pid_channel_reduce_remove, (pid,true))
            continue
        end

        try
            localresults[pid] = remotecall_wait(epmap_zeros, pid)
        catch e
            @warn "unable to initialize local reduction"
            r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, epmap_maxerrors, epmap_retries)
            epmap_eloop.interrupted = r.do_interrupt
            epmap_eloop.errored = r.do_error
            haskey(localresults, pid) && deleteat!(localresults, pid)
            if r.do_break || r.do_interrupt
                put!(epmap_eloop.pid_channel_map_remove, (pid,r.bad_pid))
            end
            continue
        end

        epmap_eloop.checkpoints[pid] = nothing

        @async while true
            @debug "map, pid=$pid, interrupted=$(epmap_eloop.interrupted), isempty(epmap_eloop.tsk_pool_todo)=$(isempty(epmap_eloop.tsk_pool_todo))"
            is_preempted = check_for_preempted(pid, epmap_preempted)
            if is_preempted || isempty(epmap_eloop.tsk_pool_todo) || epmap_eloop.interrupted
                if epmap_eloop.checkpoints[pid] !== nothing
                    push!(epmap_eloop.reduce_checkpoints, epmap_eloop.checkpoints[pid])
                end
                pop!(localresults, pid)
                pop!(epmap_eloop.checkpoints, pid)
                put!(epmap_eloop.pid_channel_map_remove, (pid,is_preempted))
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
                epmap_reporttasks && @info "running task $tsk on process $pid ($hostname); $(nworkers()) workers total; $(length(epmap_eloop.tsk_pool_todo)) tasks left in task-pool."
                yield()
                journal_start!(epmap_journal, epmap_journal_task_callback; stage="task", tsk, pid, hostname)
                remotecall_fetch(epmapreduce_fetch_apply, pid, localresults[pid], T, f, tsk, args...; kwargs...)
                journal_stop!(epmap_journal, epmap_journal_task_callback; stage="task", tsk, pid, fault=false)
                @debug "...pid=$pid ($hostname),tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(epmap_eloop.tsk_pool_todo), tsk_pool_done=$(epmap_eloop.tsk_pool_done) -!"
            catch e
                @warn "pid=$pid ($hostname), task loop, caught exception during f eval"
                journal_stop!(epmap_journal, epmap_journal_task_callback; stage="task", tsk, pid, fault=false)
                push!(epmap_eloop.tsk_pool_todo, tsk)
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, epmap_maxerrors, epmap_retries)
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
                continue # no need to checkpoint since the task failed and will be re-run
            end

            # checkpoint
            _next_checkpoint = next_checkpoint(epmapreduce_id, epmap_scratches)
            try
                @debug "running checkpoint for task $tsk on process $pid; $(nworkers()) workers total; $(length(epmap_eloop.tsk_pool_todo)) tasks left in task-pool."
                journal_start!(epmap_journal; stage="checkpoint", tsk, pid, hostname)
                remotecall_fetch(epmap_save_checkpoint, pid, _next_checkpoint, localresults[pid], T)
                journal_stop!(epmap_journal; stage="checkpoint", tsk, pid, fault=false)
                @debug "... checkpoint, pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(epmap_eloop.tsk_pool_todo) -!"
                push!(epmap_eloop.tsk_pool_done, tsk)
            catch e
                @warn "pid=$pid ($hostname), checkpoint=$(epmap_eloop.checkpoints[pid]), task loop, caught exception during save_checkpoint"
                journal_stop!(epmap_journal; stage="checkpoint", tsk, pid, fault=true)
                @debug "pushing task onto tsk_pool_todo list"
                push!(epmap_eloop.tsk_pool_todo, tsk)
                @debug "handling exception"
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, epmap_maxerrors, epmap_retries)
                @debug "done handling exception"
                epmap_eloop.interrupted = r.do_interrupt
                epmap_eloop.errored = r.do_error
                @debug "caught save checkpoint" r.do_break r.do_interrupt
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
                continue # there is no new checkpoint, and we need to keep the old checkpoint
            end

            # delete old checkpoint
            old_checkpoint,epmap_eloop.checkpoints[pid] = get(epmap_eloop.checkpoints, pid, nothing),_next_checkpoint
            try
                if old_checkpoint !== nothing
                    @debug "deleting old checkpoint"
                    journal_start!(epmap_journal; stage="checkpoint", tsk, pid, hostname)
                    epmap_keepcheckpoints || remotecall_fetch(epmap_rm_checkpoint, pid, old_checkpoint)
                    journal_stop!(epmap_journal; stage="checkpoint", tsk, pid, fault=false)
                end
            catch e
                @warn "pid=$pid ($hostname), task loop, caught exception during remove checkpoint, there may be stray check-point files that will be deleted later"
                push!(checkpoint_orphans, old_checkpoint)
                journal_stop!(epmap_journal; stage="checkpoint", tsk, pid, fault=true)
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, epmap_maxerrors, epmap_retries)
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
    end
    @debug "exited the map worker loop"
    empty!(epmap_eloop.checkpoints)

    @debug "map, emptied the checkpoints"
    isempty(checkpoint_orphans) || epmap_rm_checkpoint.(checkpoint_orphans)
    @debug "map, deleted the orphaned checkpoints"

    nothing
end

function epmapreduce_reduce!(result::T, epmap_eloop, epmap_journal;
        epmapreduce_id,
        epmap_reducer!,
        epmap_preempted,
        epmap_scratches,
        epmap_maxerrors,
        epmap_retries,
        epmap_rm_checkpoint,
        epmap_keepcheckpoints) where {T}
    @debug "entered the reduce method"
    orphans_remove = Set{String}()

    l = ReentrantLock()

    @sync while true
        @debug "reduce, interrupted=$(epmap_eloop.interrupted)"
        epmap_eloop.interrupted && break
        pid = take!(epmap_eloop.pid_channel_reduce_add)

        @debug "reduce, pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel when the reduction is done

        hostname = ""
        try
            hostname = remotecall_fetch(gethostname, pid)
        catch
            @warn "unable to determine host name for pid=$pid"
            logerror(e, Logging.Warn)
            put!(epmap_eloop.pid_channel_reduce_remove, (pid,true))
            continue
        end

        @async while true
            @debug "reduce, pid=$pid, epmap_eloop.interrupted=$(epmap_eloop.interrupted)"
            epmap_eloop.interrupted && break
            check_for_preempted(pid, epmap_preempted) && break

            epmap_eloop.reduce_checkpoints_is_dirty[pid] = true

            if length(epmap_eloop.reduce_checkpoints) < 2
                pop!(epmap_eloop.reduce_checkpoints_is_dirty, pid)
                put!(epmap_eloop.pid_channel_reduce_remove, (pid,false))
                epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                break
            end

            lock(l)
            @info "reducing from $(length(epmap_eloop.reduce_checkpoints)) checkpoints using process $pid ($(nworkers()) workers)."

            # reduce two checkpoints into a third checkpoint
            @debug "reduce, waiting for lock on pid=$pid"
            if length(epmap_eloop.reduce_checkpoints) > 1
                local checkpoint1
                try
                    checkpoint1 = popfirst!(epmap_eloop.reduce_checkpoints)
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
                    checkpoint2 = popfirst!(epmap_eloop.reduce_checkpoints)
                catch e
                    @warn "reduce, unable to pop checkpoint 2"
                    logerror(e, Logging.Warn)
                    push!(epmap_eloop.reduce_checkpoints, checkpoint1)
                    epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                    unlock(l)
                    @debug "reduce, released lock on pid=$pid"
                    continue
                end

                local checkpoint3
                try
                    checkpoint3 = next_checkpoint(epmapreduce_id, epmap_scratches)
                catch e
                    @warn "reduce, unable to create checkpoint 3"
                    logerror(e, Logging.Warn)
                    push!(epmap_eloop.reduce_checkpoints, checkpoint1, checkpoint2)
                    epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                    unlock(l)
                    @debug "reduce, released lock on pid=$pid"
                    continue
                end
            else
                epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                unlock(l)
                continue
            end
            unlock(l)
            @debug "reduce, released lock on pid=$pid"

            try
                journal_start!(epmap_journal; stage="reduce", tsk=0, pid, hostname)
                remotecall_fetch(reduce, pid, epmap_reducer!, checkpoint1, checkpoint2, checkpoint3, T)
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=false)
                push!(epmap_eloop.reduce_checkpoints, checkpoint3)
            catch e
                push!(epmap_eloop.reduce_checkpoints, checkpoint1, checkpoint2)
                @warn "pid=$pid ($hostname), reduce loop, caught exception during reduce"
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, epmap_maxerrors, epmap_retries)
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
                journal_start!(epmap_journal; stage="reduce", tsk=0, pid, hostname)
                epmap_keepcheckpoints || remotecall_fetch(epmap_rm_checkpoint, pid, checkpoint1)
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=false)
            catch e
                @warn "pid=$pid ($hostname), reduce loop, caught exception during remove checkpoint 1"
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, epmap_maxerrors, epmap_retries)
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                push!(orphans_remove, checkpoint1, checkpoint2)
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

            # remove checkpoint 2
            try
                journal_start!(epmap_journal; stage="reduce", tsk=0, pid, hostname)
                epmap_keepcheckpoints || remotecall_fetch(epmap_rm_checkpoint, pid, checkpoint2)
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=false)
            catch e
                @warn "pid=$pid ($hostname), reduce loop, caught exception during remove checkpoint 2"
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_failures, epmap_maxerrors, epmap_retries)
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                push!(orphans_remove, checkpoint2)
                epmap_eloop.interrupted = r.do_interrupt
                epmap_eloop.errored = r.do_error
                if r.do_break || r.do_interrupt
                    pop!(epmap_eloop.reduce_checkpoints_is_dirty, pid)
                    put!(epmap_eloop.pid_channel_reduce_remove, (pid,r.bad_pid))
                    epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                    break
                end
            end

            epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
        end
    end
    @debug "reduce, exited the reduce worker loop"

    for i in 1:10
        try
            epmap_reducer!(result, deserialize(epmap_eloop.reduce_checkpoints[1]))
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

    if !epmap_keepcheckpoints
        try
            epmap_rm_checkpoint(epmap_eloop.reduce_checkpoints[1])
        catch
            @warn "unable to remove final checkpoint $(epmap_eloop.reduce_checkpoints[1])"
        end
        try
            epmap_rm_checkpoint.(orphans_remove)
        catch
            @warn "unable to remove orphaned checkpoints: $(orphans_remove)"
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
    next_scratch_index(n) = (id = SID; SID = id == n ? 1 : id+1; id)
end
function next_checkpoint(id, scratch)
    joinpath(scratch[next_scratch_index(length(scratch))], string("checkpoint-", id, "-", next_checkpoint_id()))
end

function reduce(reducer!, checkpoint1, checkpoint2, checkpoint3, ::Type{T}) where {T}
    c1 = deserialize(checkpoint1)::T
    c2 = deserialize(checkpoint2)::T
    reducer!(c2, c1)
    serialize(checkpoint3, c2)
    nothing
end

default_save_checkpoint(checkpoint, localresult, ::Type{T}) where {T} = (serialize(checkpoint, fetch(localresult)::T); nothing)
restart(reducer!, orphan, localresult, ::Type{T}) where {T} = (reducer!(fetch(localresult)::T, deserialize(orphan)::T); nothing)
default_rm_checkpoint(checkpoint) = isfile(checkpoint) && rm(checkpoint)

export epmap, epmapreduce!

end
