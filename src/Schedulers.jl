module Schedulers

using Dates, Distributed, DistributedOperations, JSON, Printf, Random, Serialization, Statistics

epmap_default_addprocs = n->addprocs(n)
epmap_default_preempted = ()->false
epmap_default_init = pid->nothing

function logerror(e)
    io = IOBuffer()
    showerror(io, e)
    write(io, "\n\terror type: $(typeof(e))\n")
    for (exc, bt) in Base.catch_stack()
        showerror(io, exc, bt)
        println(io)
    end
    @warn String(take!(io))
    close(io)
end

# see https://github.com/JuliaTime/TimeZones.jl/issues/374
now_formatted() = Dates.format(now(Dates.UTC), dateformat"yyyy-mm-dd\THH:MM:SS\Z")

handle_checkpoints!(pid, localresults::Nothing, checkpoints, orphans) = nothing

function handle_checkpoints!(pid, localresults, checkpoints, orphans)
    pop!(localresults, pid)
    checkpoint = pop!(checkpoints, pid)
    checkpoint === nothing || push!(orphans, checkpoint)
    nothing
end

function handle_exception(e, pid, hostname, pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries, localresults, checkpoints, orphans)
    logerror(e)

    fails[pid] += 1
    nerrors = sum(values(fails))

    r = (do_break=false, do_interrupt=false)

    if isa(e, InterruptException)
        r = (do_break=false, do_interrupt=true)
        put!(pid_channel, -1)
        throw(e)
    elseif isa(e, ProcessExitedException)
        @warn "process with id=$pid ($hostname) exited, removing from process list."
        if haskey(Distributed.map_pid_wrkr, pid)
            @debug "pid=$pid ($hostname) is known to Distributed, calling rmprocs"
            rmprocs(pid)
        else
            @debug "pid=$pid ($hostname) is not known to Distributed, calling kill"
            try
                # required for cluster managers that require clean-up when the julia process on a worker dies:
                Distributed.kill(wrkrs[pid].manager, pid, wrkrs[pid].config)
            catch
            end
        end
        delete!(wrkrs, pid)
        r = (do_break=true, do_interrupt=false)
    elseif nerrors >= epmap_maxerrors
        put!(pid_channel, -1)
        error("too many errors on process with id=$pid ($hostname), $nerrors errors")
    elseif fails[pid] > epmap_retries+1
        @warn "too many failures on process with id=$pid ($hostname), removing from proces list"
        rmprocs(pid)
        delete!(wrkrs, pid)
        r = (do_break=true, do_interrupt=false)
    end

    handle_checkpoints!(pid, localresults, checkpoints, orphans)
    r
end
handle_exception(e, pid, hostname, pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries) = handle_exception(e, pid, hostname, pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries, nothing, nothing, nothing)

function check_for_preempted(pid, epmap_preempted)
    preempted = false
    try
        if remotecall_fetch(epmap_preempted, pid)
            rmprocs(pid)
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
        catch e
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
        catch e
            if _name ∉ (Symbol("@enter"), Symbol("@run"), :ans, :vscodedisplay)
                @debug "caught error in load_functions_on_new_workers for function $_name"
            end
        end
    end
end

# for performance metrics, track when the pid is started
const _pid_up_timestamp = Dict{Int, Float64}()

mutable struct ElasticLoop{FAddProcs<:Function,FInit<:Function,FMinWorkers<:Function,FMaxWorkers<:Function,FNWorkers<:Function,FQuantum<:Function,T}
    epmap_use_master::Bool
    initialized_pids::Vector{Int}
    used_pids::Vector{Int}
    pid_channel::Channel{Int}
    rm_pid_channel::Channel{Int}
    epmap_addprocs::FAddProcs
    epmap_init::FInit
    epmap_minworkers::FMinWorkers
    epmap_maxworkers::FMaxWorkers
    epmap_quantum::FQuantum
    epmap_nworkers::FNWorkers
    exit_on_empty::Bool
    tsk_pool_todo::Vector{T}
    tsk_pool_done::Vector{T}
    tsk_count::Int
    interrupted::Bool
end
function ElasticLoop(;
        epmap_init,
        epmap_addprocs,
        epmap_quantum,
        epmap_minworkers,
        epmap_maxworkers,
        epmap_nworkers,
        epmap_usemaster,
        exit_on_empty,
        tasks)
    _tsk_pool_todo = vec(collect(tasks))

    ElasticLoop(
        epmap_usemaster,
        epmap_usemaster ? Int[] : [1],
        epmap_usemaster ? Int[] : [1],
        Channel{Int}(32),
        Channel{Int}(32),
        epmap_addprocs,
        epmap_init,
        isa(epmap_minworkers, Function) ? epmap_minworkers : ()->epmap_minworkers,
        isa(epmap_maxworkers, Function) ? epmap_maxworkers : ()->epmap_maxworkers,
        isa(epmap_quantum, Function) ? epmap_quantum : ()->epmap_quantum,
        epmap_nworkers,
        exit_on_empty,
        _tsk_pool_todo,
        empty(_tsk_pool_todo),
        length(_tsk_pool_todo),
        false)
end

function loop(eloop::ElasticLoop)
    polling_interval = parse(Int, get(ENV, "SCHEDULERS_POLLING_INTERVAL", "1"))
    init_tasks = Dict{Int,Task}()

    while true
        @debug "checking pool, length=$(length(eloop.tsk_pool_done)), count=$(eloop.tsk_count)"
        yield()
        if length(eloop.tsk_pool_done) == eloop.tsk_count
            put!(eloop.pid_channel, -1)
            eloop.exit_on_empty && break
        end
        @debug "checking for interrupt=$(eloop.interrupted)"
        yield()
        eloop.interrupted && break

        local _epmap_nworkers,_epmap_minworkers,_epmap_maxworkers,_epmap_quantum
        try
            _epmap_nworkers,_epmap_minworkers,_epmap_maxworkers,_epmap_quantum = eloop.epmap_nworkers(),eloop.epmap_minworkers(),eloop.epmap_maxworkers(),eloop.epmap_quantum()
        catch e
            @warn "problem in Schedulers.jl elastic loop when getting nworkers,minworkers,maxworkers,quantum"
            logerror(e)
            continue
        end

        @debug "checking for new workers, nworkers=$(nworkers()), max=$_epmap_maxworkers, #todo=$(length(eloop.tsk_pool_todo))"
        yield()

        new_pids = Int[]
        for pid in workers()
            if pid ∉ eloop.used_pids
                push!(new_pids, pid)
            end
        end

        @debug "new_pids=$new_pids, nworkers=$(nworkers()), epmap_nworkers=$_epmap_nworkers"
        yield()

        for new_pid in new_pids
            push!(eloop.used_pids, new_pid)
            init_tasks[new_pid] = @async begin
                try
                    if new_pid ∉ eloop.initialized_pids
                        @debug "loading modules on $new_pid"
                        yield()
                        load_modules_on_new_workers(new_pid)
                        @debug "loading functions on $new_pid"
                        yield()
                        load_functions_on_new_workers(new_pid)
                        @debug "calling init on new worker"
                        yield()
                        eloop.epmap_init(new_pid)
                        @debug "done loading functions modules, and calling init on $new_pid"
                        yield()
                        _pid_up_timestamp[new_pid] = time()
                        push!(eloop.initialized_pids, new_pid)
                    end
                    @debug "putting pid $new_pid onto channel"
                    put!(eloop.pid_channel, new_pid)
                catch e
                    @warn "problem initializing $new_pid, removing $new_pid from cluster."
                    used_pid_index = findfirst(used_pid->used_pid == new_pid, eloop.used_pids)
                    if used_pid_index !== nothing
                        deleteat!(eloop.used_pids, used_pid_index)
                    end
                    logerror(e)
                    rmprocs(new_pid)
                end
            end
        end

        n = 0
        try
            n = min(_epmap_maxworkers-_epmap_nworkers, _epmap_quantum, length(eloop.tsk_pool_todo))
        catch e
            @warn "problem in Schedulers.jl elastic loop when computing the number of new machines to add"
            logerror(e)
        end

        @debug "add to the cluster?, n=$n, epmap_maxworkers()-epmap_nworkers()=$(_epmap_maxworkers-_epmap_nworkers), epmap_quantum=$_epmap_quantum, length(tsk_pool_todo)=$(length(eloop.tsk_pool_todo))"
        if n > 0
            try
                eloop.epmap_addprocs(n)
            catch e
                @error "problem adding new processes"
                logerror(e)
            end
        end

        @debug "checking for workers to remove"
        yield()
        try
            while isready(eloop.rm_pid_channel)
                pid = take!(eloop.rm_pid_channel)
                @debug "making sure that $pid is initialized"
                yield()
                wait(init_tasks[pid])
                _nworkers = 1 ∈ workers() ? nworkers()-1 : nworkers()
                @debug "removing worker $pid"
                yield()
                if _nworkers > _epmap_minworkers
                    rmprocs(pid)
                end
                @debug "removing worker $pid from used pids"
                deleteat_index = findfirst(used_pid->used_pid==pid, eloop.used_pids)
                if deleteat_index !== nothing
                    deleteat!(eloop.used_pids, deleteat_index)
                end
            end
        catch e
            @warn "problem in Schedulers.jl elastic loop when removing workers"
            logerror(e)
        end

        sleep(polling_interval)
    end
    nothing
end

function Base.empty!(eloop::ElasticLoop, tsk_pool_todo)
    empty!(eloop.used_pids)
    if !(eloop.epmap_use_master)
        push!(eloop.used_pids, 1)
    end
    eloop.tsk_pool_todo = tsk_pool_todo
    eloop.tsk_count = length(tsk_pool_todo)
    empty!(eloop.tsk_pool_done)
    eloop
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
    fails = Dict{Int,Int}()

    wrkrs = Dict{Int, Union{Distributed.LocalProcess, Distributed.Worker}}()

    eloop = ElasticLoop(;
        epmap_init,
        epmap_addprocs,
        epmap_quantum,
        epmap_minworkers,
        epmap_maxworkers,
        epmap_nworkers,
        epmap_usemaster,
        tasks,
        exit_on_empty = true)

    _elastic_loop = @async loop(eloop)

    journal = journal_init(tasks, epmap_journal_init_callback; reduce=false)

    # work loop
    @sync while true
        eloop.interrupted && break
        pid = take!(eloop.pid_channel)

        @debug "pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.
        pid ∈ keys(fails) && continue # only one task loop per pid

        fails[pid] = 0

        if haskey(Distributed.map_pid_wrkr, pid)
            wrkrs[pid] = Distributed.map_pid_wrkr[pid]
        else
            @warn "worker with pid=$pid is not registered"
        end
        hostname = remotecall_fetch(gethostname, pid)
        @async while true
            eloop.interrupted && break
            check_for_preempted(pid, epmap_preempted) && break

            isempty(eloop.tsk_pool_todo) && (put!(eloop.rm_pid_channel, pid); break)
            length(eloop.tsk_pool_done) == eloop.tsk_count && break
            isempty(eloop.tsk_pool_todo) && (yield(); continue)

            local tsk
            try
                tsk = popfirst!(eloop.tsk_pool_todo)
            catch
                # just in case another green-thread does popfirst! before us (unlikely)
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
                @warn "caught an exception, there have been $(fails[pid]) failure(s) on process $pid ($hostname)..."
                journal_stop!(journal, epmap_journal_task_callback; stage="task", tsk, pid, fault=true)
                push!(eloop.tsk_pool_todo, tsk)
                r = handle_exception(e, pid, hostname, eloop.pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries)
                eloop.interrupted = r.do_interrupt
                r.do_break && break
            end
        end
    end
    fetch(_elastic_loop)

    # ensure we are left with epmap_minworkers
    _workers = workers()
    1 ∈ _workers && popfirst!(_workers)
    rmprocs(_workers[1:(length(_workers) - eloop.epmap_minworkers())])

    journal_final(journal)

    journal
end

default_reducer!(x, y) = (x .+= y; nothing)
"""
    epmapreduce!(result, f, tasks, args...; epmap_kwargs..., kwargs...) -> result

where f is the map function, and `tasks` are an iterable set of tasks to map over.  The
positional arguments `args` and the named arguments `kwargs` are passed to `f` which has
the method signature: `f(localresult, f, task, args; kwargs...)`.  `localresult` is a Future
with an assoicated partial reduction.

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
* `epmap_accordion=true` shrink, and re-grow the cluster when making the transition from map to reduce
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
@everywhere f(x, tsk) = (fetch(x)::Vector{Float32} .+= tsk; nothing)
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
@everywhere f(x, tsk) = (fetch(x)::Vector{Float32} .+= tsk; nothing)
result = epmapreduce!(zeros(Float32,10), f, 1:100; epmap_scratch=container)
rmprocs(workers())
```
"""
function epmapreduce!(result::T, f, tasks, args...;
        epmap_reducer! = default_reducer!,
        epmap_zeros = nothing,
        epmapreduce_id = randstring(6),
        epmap_minworkers = nworkers,
        epmap_maxworkers = nworkers,
        epmap_usemaster = false,
        epmap_nworkers = nworkers,
        epmap_quantum = ()->32,
        epmap_accordion = true,
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

    epmap_eloop = ElasticLoop(;
        epmap_init,
        epmap_addprocs,
        epmap_quantum,
        epmap_minworkers,
        epmap_maxworkers,
        epmap_nworkers,
        epmap_usemaster,
        tasks,
        exit_on_empty = false)

    _elastic_loop = @async loop(epmap_eloop)

    checkpoints = epmapreduce_map(f, result, epmap_eloop, epmap_journal, args...;
        epmapreduce_id,
        epmap_accordion,
        epmap_reducer!,
        epmap_zeros,
        epmap_preempted,
        epmap_scratches,
        epmap_reporttasks,
        epmap_maxerrors,
        epmap_retries,
        epmap_keepcheckpoints,
        epmap_journal_task_callback,
        kwargs...)

    empty!(epmap_eloop, [1:length(checkpoints)-1;])
    epmap_eloop.exit_on_empty = true

    result = epmapreduce_reduce!(result, checkpoints, epmap_eloop, epmap_journal;
        epmapreduce_id,
        epmap_reducer!,
        epmap_preempted,
        epmap_scratches,
        epmap_reporttasks,
        epmap_maxerrors,
        epmap_retries,
        epmap_keepcheckpoints)

    fetch(_elastic_loop)

    # ensure we are left with epmap_minworkers
    _workers = workers()
    1 ∈ _workers && popfirst!(_workers)
    rmprocs(_workers[1:(length(_workers) - epmap_eloop.epmap_minworkers())])

    journal_final(epmap_journal)
    journal_write(epmap_journal, epmap_journalfile)

    result
end

function epmapreduce_map(f, results::T, epmap_eloop, epmap_journal, args...;
        epmapreduce_id,
        epmap_accordion,
        epmap_reducer!,
        epmap_zeros,
        epmap_preempted,
        epmap_scratches,
        epmap_reporttasks,
        epmap_maxerrors,
        epmap_retries,
        epmap_keepcheckpoints,
        epmap_journal_task_callback,
        kwargs...) where {T}
    fails = Dict{Int,Int}()

    wrkrs = Dict{Int, Union{Distributed.LocalProcess, Distributed.Worker}}()

    localresults = Dict{Int, Future}()
    checkpoints = Dict{Int, Any}()
    orphans_compute = Set{Any}()
    orphans_remove = Set{Any}()

    # task loop
    epmap_reporttasks && @info "task loop..."
    @sync while true
        epmap_eloop.interrupted && break
        pid = take!(epmap_eloop.pid_channel)

        @debug "pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.
        pid ∈ keys(localresults) && break # task loop has already run for this pid

        localresults[pid] = remotecall(epmap_zeros, pid)
        checkpoints[pid] = nothing

        fails[pid] = 0

        if haskey(Distributed.map_pid_wrkr, pid)
            wrkrs[pid] = Distributed.map_pid_wrkr[pid]
        else
            @warn "worker with pid=$pid is not registered"
        end
        hostname = remotecall_fetch(gethostname, pid)
        @async while true
            epmap_eloop.interrupted && break
            check_for_preempted(pid, epmap_preempted) && break

            if isempty(epmap_eloop.tsk_pool_todo) && isempty(orphans_compute) && isempty(orphans_remove)
                epmap_accordion && put!(epmap_eloop.rm_pid_channel, pid)
                break
            end
            length(epmap_eloop.tsk_pool_done) == epmap_eloop.tsk_count && break
            isempty(epmap_eloop.tsk_pool_todo) && length(epmap_eloop.tsk_pool_done) != epmap_eloop.tsk_count && isempty(orphans_compute) && isempty(orphans_remove) && (yield(); continue)

            # re-start logic, reduce-in orphaned check-points
            if !isempty(orphans_compute)
                local orphan
                try
                    @debug "restart from check-point."
                    orphan = pop!(orphans_compute)
                    journal_start!(epmap_journal; stage="restart", tsk=0, pid, hostname)
                    remotecall_fetch(restart, pid, epmap_reducer!, orphan, localresults[pid], T)
                    journal_stop!(epmap_journal; stage="restart", tsk=0, pid, fault=false)
                catch e
                    @warn "caught restart error, reduce-in orphan check-point"
                    journal_stop!(epmap_journal; stage="restart", tsk=0, pid, fault=true)
                    push!(orphans_compute, orphan)
                    r = handle_exception(e, pid, hostname, epmap_eloop.pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries)
                    epmap_eloop.interrupted = r.do_interrupt
                    r.do_break && break
                end
                
                try
                    @debug "check-point restart."
                    _next_checkpoint = next_checkpoint(epmapreduce_id, epmap_scratches)
                    journal_start!(epmap_journal; stage="restart", tsk=0, pid, hostname)
                    remotecall_fetch(save_checkpoint, pid, _next_checkpoint, localresults[pid], T)
                    journal_stop!(epmap_journal; stage="restart", tsk=0, pid, fault=false)
                    old_checkpoint,checkpoints[pid] = checkpoints[pid],_next_checkpoint
                    push!(orphans_remove, orphan)
                    old_checkpoint === nothing || push!(orphans_remove, old_checkpoint)
                catch e
                    @warn "caught restart error, creating check-point."
                    journal_stop!(epmap_journal; stage="restart", tsk=0, pid, fault=true)
                    push!(orphans_compute, orphan)
                    r = handle_exception(e, pid, hostname, epmap_eloop.pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries)
                    epmap_eloop.interrupted = r.do_interrupt
                    r.do_break && break
                end

                isempty(orphans_compute) || continue
            end

            # re-start logic, clean-up orphaned, but already reduced, check-points
            if !isempty(orphans_remove)
                local orphan
                try
                    @debug "removing already reduced orphaned check-point"
                    orphan = pop!(orphans_remove)
                    journal_start!(epmap_journal; stage="restart", tsk=0, pid, hostname)
                    epmap_keepcheckpoints || remotecall_fetch(rm_checkpoint, pid, orphan)
                    journal_stop!(epmap_journal; stage="retart", tsk=0, pid, fault=false)
                catch e
                    @warn "caught restart error, clean-up"
                    journal_stop!(epmap_journal; stage="retart", tsk=0, pid, fault=true)
                    push!(orphans_remove, orphan)
                    r = handle_exception(e, pid, hostname, epmap_eloop.pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries)
                    epmap_eloop.interrupted = r.do_interrupt
                    r.do_break && break
                end

                isempty(orphans_remove) || continue
            end

            length(epmap_eloop.tsk_pool_done) == epmap_eloop.tsk_count && break
            isempty(epmap_eloop.tsk_pool_todo) && (yield(); continue)

            # get next task
            local tsk
            try
                tsk = popfirst!(epmap_eloop.tsk_pool_todo)
            catch
                # just in case another green-thread does popfirst! before us (unlikely)
                yield()
                continue
            end

            # compute and reduce
            try
                epmap_reporttasks && @info "running task $tsk on process $pid ($hostname); $(nworkers()) workers total; $(length(epmap_eloop.tsk_pool_todo)) tasks left in task-pool."
                yield()
                journal_start!(epmap_journal, epmap_journal_task_callback; stage="task", tsk, pid, hostname)
                remotecall_fetch(f, pid, localresults[pid], tsk, args...; kwargs...)
                journal_stop!(epmap_journal, epmap_journal_task_callback; stage="task", tsk, pid, fault=false)
                @debug "...pid=$pid ($hostname),tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(epmap_eloop.tsk_pool_todo), tsk_pool_done=$(epmap_eloop.tsk_pool_done) -!"
            catch e
                @warn "pid=$pid ($hostname), task loop, caught exception during f eval"
                journal_stop!(epmap_journal, epmap_journal_task_callback; stage="task", tsk, pid, fault=false)
                push!(epmap_eloop.tsk_pool_todo, tsk)
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries)
                epmap_eloop.interrupted = r.do_interrupt
                r.do_break && break
                continue # no need to checkpoint since the task failed
            end

            # checkpoint
            _next_checkpoint = next_checkpoint(epmapreduce_id, epmap_scratches)
            try
                @debug "running checkpoint for task $tsk on process $pid; $(nworkers()) workers total; $(length(epmap_eloop.tsk_pool_todo)) tasks left in task-pool."
                journal_start!(epmap_journal; stage="checkpoint", tsk, pid, hostname)
                remotecall_fetch(save_checkpoint, pid, _next_checkpoint, localresults[pid], T)
                journal_stop!(epmap_journal; stage="checkpoint", tsk, pid, fault=false)
                @debug "... checkpoint, pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(epmap_eloop.tsk_pool_todo) -!"
                push!(epmap_eloop.tsk_pool_done, tsk)
            catch e
                @warn "pid=$pid ($hostname), task loop, caught exception during save_checkpoint"
                journal_stop!(epmap_journal; stage="checkpoint", tsk, pid, fault=true)
                push!(epmap_eloop.tsk_pool_todo, tsk)
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries, localresults, checkpoints, orphans_compute)
                epmap_eloop.interrupted = r.do_interrupt
                r.do_break && break
            end

            # delete old checkpoint
            old_checkpoint,checkpoints[pid] = get(checkpoints, pid, nothing),_next_checkpoint
            try
                if old_checkpoint !== nothing
                    journal_start!(epmap_journal; stage="checkpoint", tsk, pid, hostname)
                    epmap_keepcheckpoints || remotecall_fetch(rm_checkpoint, pid, old_checkpoint)
                    journal_stop!(epmap_journal; stage="checkpoint", tsk, pid, fault=false)
                end
            catch e
                @warn "pid=$pid ($hostname), task loop, caught exception during rm_checkpoint"
                journal_stop!(epmap_journal; stage="checkpoint", tsk, pid, fault=true)
                old_checkpoint === nothing || push!(orphans_remove, old_checkpoint)
                handle_exception(e, pid, hostname, epmap_eloop.pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries, localresults, checkpoints, orphans_compute)
                epmap_eloop.interrupted = r.do_interrupt
                r.do_break && break
            end
        end
    end
    epmap_reporttasks && @info "...done task loop"
    filter!(checkpoint->checkpoint !== nothing, [collect(values(checkpoints)); orphans_compute...])
end

function epmapreduce_reduce!(result::T, checkpoints, epmap_eloop, epmap_journal;
        epmapreduce_id,
        epmap_reducer!,
        epmap_preempted,
        epmap_scratches,
        epmap_reporttasks,
        epmap_maxerrors,
        epmap_retries,
        epmap_keepcheckpoints) where {T}
    fails = Dict{Int,Int}()

    wrkrs = Dict{Int, Union{Distributed.LocalProcess, Distributed.Worker}}()

    orphans_remove = Set{String}()

    epmap_reporttasks && @info "reduce loop..."
    @sync while true
        epmap_eloop.interrupted && break
        pid = take!(epmap_eloop.pid_channel)

        @debug "pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.

        fails[pid] = 0

        hostname = ""
        try
            hostname = remotecall_fetch(gethostname, pid)
        catch
            @warn "unable to get hostname for pid=$pid ($hostname)"
        end

        if haskey(Distributed.map_pid_wrkr, pid)
            wrkrs[pid] = Distributed.map_pid_wrkr[pid]
        else
            @warn "worker with pid=$pid ($hostname) is not registered"
        end

        @async while true
            epmap_eloop.interrupted && break
            check_for_preempted(pid, epmap_preempted) && break

            length(epmap_eloop.tsk_pool_done) == epmap_eloop.tsk_count && break
            length(checkpoints) < 2 && (yield(); continue)

            if isempty(epmap_eloop.tsk_pool_todo)
                continue
            end

            local tsk
            try
                tsk = popfirst!(epmap_eloop.tsk_pool_todo)
            catch
                continue
            end

            local checkpoint1,checkpoint2,checkpoint3
            try
                checkpoint1,checkpoint2,checkpoint3 = popfirst!(checkpoints),popfirst!(checkpoints),next_checkpoint(epmapreduce_id, epmap_scratches)
            catch
                continue
            end
            
            try
                journal_start!(epmap_journal; stage="reduce", tsk=0, pid, hostname)
                remotecall_fetch(reduce, pid, epmap_reducer!, checkpoint1, checkpoint2, checkpoint3, T)
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=false)
                push!(checkpoints, checkpoint3)
                push!(epmap_eloop.tsk_pool_done, tsk)
            catch e
                @warn "pid=$pid ($hostname), reduce loop, caught exception during reduce"
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                push!(checkpoints, checkpoint1, checkpoint2)
                push!(epmap_eloop.tsk_pool_todo, tsk)
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries)
                epmap_eloop.interrupted = r.do_interrupt
                r.do_break && break
                continue
            end

            try
                journal_start!(epmap_journal; stage="reduce", tsk=0, pid, hostname)
                epmap_keepcheckpoints || remotecall_fetch(rm_checkpoint, pid, checkpoint1)
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=false)
            catch e
                @warn "pid=$pid ($hostname), reduce loop, caught exception during rm_checkpoint 1"
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                push!(orphans_remove, checkpoint1, checkpoint2)
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries)
                epmap_eloop.interrupted = r.do_interrupt
                r.do_break && break
                continue
            end
            
            try
                journal_start!(epmap_journal; stage="reduce", tsk=0, pid, hostname)
                epmap_keepcheckpoints || remotecall_fetch(rm_checkpoint, pid, checkpoint2)
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=false)
            catch e
                @warn "pid=$pid ($hostname), reduce loop, caught exception during rm_checkpoint 2"
                journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                push!(orphans_remove, checkpoint2)
                r = handle_exception(e, pid, hostname, epmap_eloop.pid_channel, wrkrs, fails, epmap_maxerrors, epmap_retries)
                epmap_eloop.interrupted = r.do_interrupt
                r.do_break && break
            end
        end
    end
    epmap_reporttasks && @info "...done reduce loop."

    epmap_reducer!(result, deserialize(checkpoints[1]))
    if !epmap_keepcheckpoints
        rm_checkpoint(checkpoints[1])
        rm_checkpoint.(orphans_remove)
    end

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

save_checkpoint(checkpoint, localresult, ::Type{T}) where {T} = (serialize(checkpoint, fetch(localresult)::T); nothing)
restart(reducer!, orphan, localresult, ::Type{T}) where {T} = (reducer!(fetch(localresult)::T, deserialize(orphan)::T); nothing)
rm_checkpoint(checkpoint) = isfile(checkpoint) && rm(checkpoint)

export epmap, epmapreduce!

end
