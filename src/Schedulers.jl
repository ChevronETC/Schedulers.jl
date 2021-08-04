module Schedulers

using Dates, Distributed, DistributedOperations, Printf, Random, Serialization, Statistics

epmap_default_addprocs = n->addprocs(n)
epmap_default_preempted = ()->false
epmap_default_init = pid->nothing

function logerror(e)
    io = IOBuffer()
    for (exc, bt) in Base.catch_stack()
        showerror(io, exc, bt)
        println(io)
    end
    @warn String(take!(io))
    close(io)
end

function journal_init(tsks)
    journal = Dict()
    for tsk in tsks
        journal[tsk] = Dict[]
    end
    journal
end
journal_start!(journal, tsk; pid, hostname) = push!(journal[tsk], Dict("pid"=>pid, "hostname"=>hostname, "start"=>Dates.format(now(Dates.UTC), "yyyy-mm-ddTHH:MM:SS")))

function journal_stop!(journal, tsk; fault)
    journal[tsk][end]["status"] = fault ? "failed" : "succeeded"
    journal[tsk][end]["stop"] = Dates.format(now(Dates.UTC), "yyyy-mm-ddTHH:MM:SS")
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

function elastic_loop(pid_channel, rm_pid_channel, tsk_pool_done, tsk_pool_todo, tsk_count, interrupted, epmap_minworkers, epmap_maxworkers, epmap_quantum, epmap_addprocs, epmap_init, epmap_nworkers, epmap_usemaster)
    pids = epmap_usemaster ? Int[] : [1]
    init_tasks = Dict{Int,Task}()

    while true
        @debug "checking pool, length=$(length(tsk_pool_done)), count=$tsk_count"
        yield()
        length(tsk_pool_done) == tsk_count && (put!(pid_channel, -1); break)
        @debug "checking for interrupt=$interrupted"
        yield()
        interrupted && break

        local _epmap_nworkers,_epmap_minworkers,_epmap_maxworkers,_epmap_quantum
        try
            _epmap_nworkers,_epmap_minworkers,_epmap_maxworkers,_epmap_quantum = epmap_nworkers(),epmap_minworkers(),epmap_maxworkers(),epmap_quantum()
        catch e
            @warn "problem in Schedulers.jl elastic loop when getting nworkers,minworkers,maxworkers,quantum"
            logerror(e)
            continue
        end

        @debug "checking for new workers, nworkers=$(nworkers()), max=$_epmap_maxworkers, #todo=$(length(tsk_pool_todo))"
        yield()

        new_pids = Int[]
        for pid in workers()
            if pid ∉ pids
                push!(new_pids, pid)
                push!(pids, pid)
            end
        end

        @debug "new_pids=$new_pids, nworkers=$(nworkers()), epmap_nworkers=$_epmap_nworkers"
        yield()

        for new_pid in new_pids
            init_tasks[new_pid] = @async begin
                try
                    @debug "loading modules on $new_pid"
                    yield()
                    load_modules_on_new_workers(new_pid)
                    @debug "loading functions on $new_pid"
                    yield()
                    load_functions_on_new_workers(new_pid)
                    @debug "calling init on new worker"
                    yield()
                    epmap_init(new_pid)
                    @debug "done loading functions modules, and calling init on $new_pid"
                    yield()
                    _pid_up_timestamp[new_pid] = time()
                    put!(pid_channel, new_pid)
                catch e
                    @warn "problem running epmap_init on $new_pid, removing $new_pid from cluster."
                    logerror(e)
                    rmprocs(new_pid)
                end
            end
        end

        n = 0
        try
            n = min(_epmap_maxworkers-_epmap_nworkers, _epmap_quantum, length(tsk_pool_todo))
        catch e
            @warn "problem in Schedulers.jl elastic loop when computing the number of new machines to add"
            logerror(e)
        end

        @debug "add to the cluster?, n=$n, epmap_maxworkers()-epmap_nworkers()=$(_epmap_maxworkers-_epmap_nworkers), epmap_quantum=$_epmap_quantum, length(tsk_pool_todo)=$(length(tsk_pool_todo))"
        if n > 0
            try
                epmap_addprocs(n)
            catch e
                @error "problem adding new processes"
                logerror(e)
            end
        end

        @debug "checking for workers to remove"
        yield()
        try
            while isready(rm_pid_channel)
                pid = take!(rm_pid_channel)
                @debug "making sure that $pid is initialized"
                yield()
                wait(init_tasks[pid])
                _nworkers = 1 ∈ workers() ? nworkers()-1 : nworkers()
                @debug "removing worker $pid"
                yield()
                if _nworkers > _epmap_minworkers
                    rmprocs(pid)
                end
            end
        catch e
            @warn "problem in Schedulers.jl elastic loop when removing workers"
            logerror(e)
        end

        sleep(10)
    end
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
        kwargs...)
    tsk_pool_todo = collect(tasks)
    tsk_pool_done = []
    tsk_count = length(tsk_pool_todo)

    pid_channel = Channel{Int}(32)
    rm_pid_channel = Channel{Int}(32)

    fails = Dict{Int,Int}()

    wrkrs = Dict{Int, Distributed.Worker}()

    interrupted = false

    _epmap_minworkers = isa(epmap_minworkers, Function) ? epmap_minworkers : ()->epmap_minworkers
    _epmap_maxworkers = isa(epmap_maxworkers, Function) ? epmap_maxworkers : ()->epmap_maxworkers
    _epmap_quantum = isa(epmap_quantum, Function) ? epmap_quantum : ()->epmap_quantum

    _elastic_loop = @async elastic_loop(pid_channel, rm_pid_channel, tsk_pool_done, tsk_pool_todo, tsk_count, interrupted, _epmap_minworkers, _epmap_maxworkers, _epmap_quantum, epmap_addprocs, epmap_init, epmap_nworkers, epmap_usemaster)

    journal = journal_init(tasks)

    # work loop
    @sync while true
        interrupted && break
        pid = take!(pid_channel)

        @debug "pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.

        fails[pid] = 0

        if haskey(Distributed.map_pid_wrkr, pid)
            wrkrs[pid] = Distributed.map_pid_wrkr[pid]
        else
            @warn "worker with pid=$pid is not registered"
        end
        hostname = remotecall_fetch(gethostname, pid)
        @async while true
            interrupted && break
            try
                if remotecall_fetch(epmap_preempted, pid)
                    rmprocs(pid)
                    break
                end
            catch e
                @debug "unable to call preempted method"
            end

            isempty(tsk_pool_todo) && (put!(rm_pid_channel, pid); break)
            length(tsk_pool_done) == tsk_count && break
            isempty(tsk_pool_todo) && (yield(); continue)

            local tsk
            try
                tsk = popfirst!(tsk_pool_todo)
            catch
                # just in case another green-thread does popfirst! before us (unlikely)
                yield()
                continue
            end

            try
                epmap_reporttasks && @info "running task $tsk on process $pid; $(nworkers()) workers total; $(length(tsk_pool_todo)) tasks left in task-pool."
                yield()
                journal_start!(journal, tsk; pid, hostname)
                remotecall_fetch(f, pid, tsk, args...; kwargs...)
                journal_stop!(journal, tsk; fault=false)
                push!(tsk_pool_done, tsk)
                @debug "...pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$tsk_pool_todo, tsk_pool_done=$tsk_pool_done -!"
                yield()
            catch e
                journal_stop!(journal, tsk; fault=true)
                fails[pid] += 1
                nerrors = sum(values(fails))
                @warn "caught an exception, there have been $(fails[pid]) failure(s) on process $pid..."
                logerror(e)
                push!(tsk_pool_todo, tsk)
                if isa(e, InterruptException)
                    interrupted = true
                    put!(pid_channel, -1)
                    throw(e)
                elseif isa(e, ProcessExitedException)
                    @warn "process with id=$pid exited, removing from process list"
                    if haskey(Distributed.map_pid_wrkr, pid)
                        @debug "pid=$pid is known to Distributed, calling rmprocs"
                        rmprocs(pid)
                    else
                        @debug "pid=$pid is not known to Distributed, calling kill"
                        try
                            # required for cluster managers that require clean-up when the julia process on a worker dies:
                            Distributed.kill(wrkrs[pid].manager, pid, wrkrs[pid].config)
                        catch
                        end
                    end
                    delete!(wrkrs, pid)
                    break
                elseif nerrors >= epmap_maxerrors
                    interrupted = true
                    put!(pid_channel, -1)
                    error("too many errors, $nerrors errors")
                elseif fails[pid] > epmap_retries
                    @warn "too many failures on process with id=$pid, removing from process list"
                    rmprocs(pid)
                    break
                end
            end
        end
    end
    fetch(_elastic_loop)

    # ensure we are left with epmap_minworkers
    _workers = workers()
    1 ∈ _workers && popfirst!(_workers)
    rmprocs(_workers[1:(length(_workers) - _epmap_minworkers())])

    journal
end

default_reducer!(x, y) = x .+= y
"""
    epmapreduce!(result, f, tasks, args...; epmap_kwargs..., kwargs...) -> result

where f is the map function, and `tasks` are an iterable set of tasks to map over.  The
positional arguments `args` and the named arguments `kwargs` are passed to `f` which has
the method signature: `f(localresult, f, task, args; kwargs...)`.  `localresult` is a Future
with an assoicated partial reduction.

## epmap_kwargs
* `epmap_reducer! = default_reducer!` the method used to reduce the result. The default is `(x,y)->(x .+= y)`
* `epmap_zeros = ()->zeros(eltype(result), size(result))` the method used to initiaize partial reductions
* `epmap_minworkers=nworkers` method giving the minimum number of workers to elastically shrink to
* `epmap_maxworkers=nworkers` method giving the maximum number of workers to elastically expand to
* `epmap_usemaster=false` assign tasks to the master process?
* `epmap_nworkers=nworkers` the number of machines currently provisioned for work[1]
* `epmap_quantum=()->32` the maximum number of workers to elastically add at a time
* `epmap_addprocs=n->addprocs(n)` method for adding n processes (will depend on the cluster manager being used)
* `epmap_init=pid->nothing` after starting a worker, this method is run on that worker.
* `epmap_scratch="/scratch"` storage location accesible to all cluster machines (e.g NFS, Azure blobstore,...)
* `epmap_reporttasks=true` log task assignment

## Notes
[1] The number of machines provisioined may be greater than the number of workers in the cluster since with
some cluster managers, there may be a delay between the provisioining of a machine, and when it is added to the
Julia cluster.
[2] For example, on Azure Cloud a SPOT instance will be pre-empted if someone is willing to pay more for it

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
        epmap_addprocs = epmap_default_addprocs,
        epmap_init = epmap_default_init,
        epmap_scratch = "/scratch",
        epmap_reporttasks = true,
        kwargs...) where {T,N}
    isdir(epmap_scratch) || mkpath(epmap_scratch)
    if epmap_zeros == nothing
        epmap_zeros = ()->zeros(eltype(result), size(result))::T
    end

    _epmap_minworkers = isa(epmap_minworkers, Function) ? epmap_minworkers : ()->epmap_minworkers
    _epmap_maxworkers = isa(epmap_maxworkers, Function) ? epmap_maxworkers : ()->epmap_maxworkers
    _epmap_quantum = isa(epmap_quantum, Function) ? epmap_quantum : ()->epmap_quantum

    empty!(_timers)
    checkpoints = epmapreduce_map(f, tasks, result, args...;
        epmapreduce_id=epmapreduce_id, epmap_reducer! = epmap_reducer!, epmap_zeros=epmap_zeros, epmap_minworkers=_epmap_minworkers, epmap_maxworkers=_epmap_maxworkers, epmap_usemaster=epmap_usemaster, epmap_nworkers=epmap_nworkers, epmap_quantum=_epmap_quantum, epmap_addprocs=epmap_addprocs, epmap_init=epmap_init, epmap_scratch=epmap_scratch, epmap_reporttasks=epmap_reporttasks, kwargs...)
    epmapreduce_reduce!(result, checkpoints;
        epmapreduce_id=epmapreduce_id, epmap_reducer! = epmap_reducer!, epmap_minworkers=_epmap_minworkers, epmap_maxworkers=_epmap_maxworkers, epmap_usemaster=epmap_usemaster, epmap_nworkers=epmap_nworkers, epmap_quantum=_epmap_quantum, epmap_addprocs=epmap_addprocs, epmap_init=epmap_init, epmap_scratch=epmap_scratch, epmap_reporttasks=epmap_reporttasks)
end

function epmapreduce_map(f, tasks, results::T, args...;
        epmapreduce_id,
        epmap_reducer!,
        epmap_zeros,
        epmap_minworkers,
        epmap_maxworkers,
        epmap_usemaster,
        epmap_nworkers,
        epmap_quantum,
        epmap_addprocs,
        epmap_init,
        epmap_scratch,
        epmap_reporttasks,
        kwargs...) where {T}
    tsk_pool_todo = collect(tasks)
    tsk_pool_done = []
    interrupted = false
    tsk_count = length(tsk_pool_todo)

    pid_channel = Channel{Int}(32)
    rm_pid_channel = Channel{Int}(32)

    _elastic_loop = @async elastic_loop(pid_channel, rm_pid_channel, tsk_pool_done, tsk_pool_todo, tsk_count, interrupted, epmap_minworkers, epmap_maxworkers, epmap_quantum, epmap_addprocs, epmap_init, epmap_nworkers, epmap_usemaster)

    localresults = Dict{Int, Future}()
    checkpoints = Dict{Int, Any}()
    orphans_compute = Set{Any}()
    orphans_remove = Set{Any}()

    _timers["map"] = Dict{Int, Dict{String,Float64}}()
    tic_cumulative = Dict{Int,Float64}()

    # task loop
    epmap_reporttasks && @info "task loop..."
    @sync while true
        pid = take!(pid_channel)
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.

        localresults[pid] = remotecall(epmap_zeros, pid)
        checkpoints[pid] = nothing

        _timers["map"][pid] = Dict("cumulative"=>0.0, "restart"=>0.0, "f"=>0.0, "checkpoint"=>0.0, "uptime"=>0.0)

        tic_cumulative[pid] = time()
        @async while true
            isempty(tsk_pool_todo) && length(tsk_pool_done) == tsk_count && isempty(orphans_compute) && isempty(orphans_remove) && break
            _timers["map"][pid]["uptime"] = time() - _pid_up_timestamp[pid]
            isempty(tsk_pool_todo) && length(tsk_pool_done) != tsk_count && isempty(orphans_compute) && isempty(orphans_remove) && (yield(); continue)
            _timers["map"][pid]["cumulative"] = time() - tic_cumulative[pid]

            # re-start logic, reduce-in orphaned check-points
            if !isempty(orphans_compute)
                local orphan
                try
                    orphan = pop!(orphans_compute)
                    _timers["map"][pid]["restart"] += @elapsed remotecall_fetch(restart, pid, epmap_reducer!, orphan, localresults[pid], T)
                catch e
                    @warn "caught restart error, reduce-in orhpan checkpoing"
                    push!(orphans_compute, orphan)
                    isa(e, ProcessExitedException) && (rmprocs(pid); break)
                    error("TODO")
                    throw(e)
                end
                
                try
                    _next_checkpoint = next_checkpoint(epmapreduce_id, epmap_scratch)
                    _timers["map"][pid]["restart"] += @elapsed remotecall_fetch(save_checkpoint, pid, _next_checkpoint, localresults[pid], T)
                    old_checkpoint,checkpoints[pid] = checkpoints[pid],_next_checkpoint
                    push!(orphans_remove, orphan)
                    old_checkpoint == nothing || push!(orphans_remove, old_checkpoint)
                catch e
                    @warn "caught restart error, checkpointing"
                    showerror(stdout, e)
                    isa(e, ProcessExitedException) && (push!(orphans_compute, orphan); rmprocs(pid); break)
                    error("TODO")
                    throw(e)
                end

                isempty(orphans_compute) || continue
            end

            # re-start logic, clean-up orphaned, but already reduced, check-points
            if !isempty(orphans_remove)
                local orphan
                try
                    orphan = pop!(orphans_remove)
                    _timers["map"][pid]["restart"] += @elapsed remotecall_fetch(rm_checkpoint, pid, orphan)
                catch e
                    @warn "caught restart error, clean-up"
                    push!(orphans_remove, orphan)
                    isa(e, ProcessExitedException) && (rmprocs(pid); break)
                    error("TODO")
                    throw(e)
                end

                isempty(orphans_remove) || continue
            end

            length(tsk_pool_done) == tsk_count && break
            isempty(tsk_pool_todo) && (yield(); continue)

            # get next task
            local tsk
            try
                tsk = popfirst!(tsk_pool_todo)
            catch
                yield()
                continue
            end

            # compute and reduce
            try
                epmap_reporttasks && @info "running task $tsk on process $pid; $(nworkers()) workers total; $(length(tsk_pool_todo)) tasks left in task-pool."
                _timers["map"][pid]["f"] += @elapsed remotecall_fetch(f, pid, localresults[pid], tsk, args...; kwargs...)
                @debug "... task, pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$tsk_pool_todo -!"
            catch e
                @warn "pid=$pid, task loop, caught exception during f eval"
                push!(tsk_pool_todo, tsk)
                isa(e, ProcessExitedException) && (handle_process_exited(pid, localresults, checkpoints, orphans_compute); break)
                showerror(stdout, e)
                error("TODO")
                throw(e)
            end

            # checkpoint
            _next_checkpoint = next_checkpoint(epmapreduce_id, epmap_scratch)
            try
                @debug "running checkpoint for task $tsk on process $pid; $(nworkers()) workers total; $(length(tsk_pool_todo)) tasks left in task-pool."
                _timers["map"][pid]["checkpoint"] += @elapsed remotecall_fetch(save_checkpoint, pid, _next_checkpoint, localresults[pid], T)
                @debug "... checkpoint, pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$tsk_pool_todo -!"
                push!(tsk_pool_done, tsk)
            catch e
                @warn "pid=$pid, task loop, caught exception during save_checkpoint"
                isa(e, ProcessExitedException) && (push!(tsk_pool_todo, tsk); handle_process_exited(pid, localresults, checkpoints, orphans_compute); break)
                showerror(stdout, e)
                error("TODO")
                throw(e)
            end

            # delete old checkpoint
            old_checkpoint,checkpoints[pid] = checkpoints[pid],_next_checkpoint
            try
                if old_checkpoint != nothing
                    _timers["map"][pid]["checkpoint"] += @elapsed remotecall_fetch(rm_checkpoint, pid, old_checkpoint)
                end
            catch e
                @warn "pid=$pid, task loop, caught exception during rm_checkpoint"
                old_checkpoint == nothing || push!(orphans_remove, old_checkpoint)
                isa(e, ProcessExitedException) && (handle_process_exited(pid, localresults, checkpoints, orphans_remove); break)
                error("TODO")
                throw(e)
            end
        end
    end
    fetch(_elastic_loop)
    epmap_reporttasks && @info "...done task loop"
    filter!(checkpoint->checkpoint != nothing, [collect(values(checkpoints)); orphans_compute...])
end

function epmapreduce_reduce!(result::T, checkpoints;
        epmapreduce_id,
        epmap_reducer!,
        epmap_minworkers,
        epmap_maxworkers,
        epmap_usemaster,
        epmap_nworkers,
        epmap_quantum,
        epmap_addprocs,
        epmap_init,
        epmap_scratch,
        epmap_reporttasks) where {T}
    epmap_reporttasks && @info "reduce loop..."
    n_checkpoints = length(checkpoints)
    # reduce loop, tsk_pool_todo and tsk_pool_done are not really needed, but lets us reuse the elastic_loop method
    tsk_pool_todo = [1:n_checkpoints-1;]
    tsk_pool_done = Int[]
    ntsks = length(tsk_pool_todo)
    interrupted = false

    pid_channel = Channel{Int}(32)
    rm_pid_channel = Channel{Int}(32)
    
    _elastic_loop = @async elastic_loop(pid_channel, rm_pid_channel, tsk_pool_done, tsk_pool_todo, ntsks, interrupted, epmap_minworkers, epmap_maxworkers, epmap_quantum, epmap_addprocs, epmap_init, epmap_nworkers, epmap_usemaster)

    _timers["reduce"] = Dict{Int, Dict{String,Float64}}()

    orphans_remove = Set{String}()

    @sync while true
        pid = take!(pid_channel)
        @debug "pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.

        _timers["reduce"][pid] = Dict("cumulative"=>0.0, "reduce"=>0.0, "IO"=>0.0, "cleanup"=>0.0, "uptime"=>0.0)

        tic_cumulative = time()
        @async while true
            _timers["reduce"][pid]["cumulative"] = time() - tic_cumulative
            _timers["reduce"][pid]["uptime"] = time() - _pid_up_timestamp[pid]

            length(tsk_pool_done) == ntsks && break
            length(checkpoints) < 2 && (yield(); continue)

            tsk = popfirst!(tsk_pool_todo)

            local checkpoint1,checkpoint2,checkpoint3
            try
                checkpoint1,checkpoint2,checkpoint3 = popfirst!(checkpoints),popfirst!(checkpoints),next_checkpoint(epmapreduce_id, epmap_scratch)
            catch
                continue
            end
            
            try
                t_io, t_sum = remotecall_fetch(reduce, pid, epmap_reducer!, checkpoint1, checkpoint2, checkpoint3, T)
                _timers["reduce"][pid]["IO"] += t_io
                _timers["reduce"][pid]["reduce"] += t_sum
                push!(checkpoints, checkpoint3)
                push!(tsk_pool_done, tsk)
            catch e
                @warn "pid=$pid, reduce loop, caught exception during reduce"
                showerror(stdout, e)
                push!(checkpoints, checkpoint1, checkpoint2)
                push!(tsk_pool_todo, tsk)
                isa(e, ProcessExitedException) && (rmprocs(pid); break)
                error("TODO")
                throw(e)
            end

            try
                _timers["reduce"][pid]["cleanup"] += @elapsed remotecall_fetch(rm_checkpoint, pid, checkpoint1)
            catch e
                @warn "pid=$pid, reduce loop, caught exception during rm_checkpoint 1"
                push!(orphans_remove, checkpoint1, checkpoint2)
                isa(e, ProcessExitedException) && (rmprocs(pid); break)
                error("TODO")
                throw(e)
            end
            
            try
                _timers["reduce"][pid]["cleanup"] += @elapsed remotecall_fetch(rm_checkpoint, pid, checkpoint2)
            catch e
                @warn "pid=$pid, reduce loop, caught exception during rm_checkpoint 2"
                push!(orphans_remove, checkpoint2)
                isa(e, ProcessExitedException) && (rmprocs(pid) && break)
                error("TODO")
                throw(e)
            end
        end
    end
    fetch(_elastic_loop)
    epmap_reporttasks && @info "...done reduce loop."

    # ensure we are left with epmap_minworkers
    _workers = workers()
    1 ∈ _workers && popfirst!(_workers)
    rmprocs(_workers[1:(length(_workers) - epmap_minworkers())])

    epmap_reducer!(result, deserialize(checkpoints[1]))
    rm_checkpoint(checkpoints[1])
    rm_checkpoint.(orphans_remove)
    result
end

const _timers = Dict{String,Dict{Int,Dict{String,Float64}}}()

function timers_analysis_map(filename)
    pids = collect(keys(Schedulers._timers["map"]))

    f = [Schedulers._timers["map"][pid]["f"] for pid in pids]
    μ_f = mean(f)
    σ_f = sqrt(var(f))

    checkpoint = [Schedulers._timers["map"][pid]["checkpoint"] for pid in pids]
    μ_checkpoint = mean(checkpoint)
    σ_checkpoint = sqrt(var(checkpoint))

    restart = [Schedulers._timers["map"][pid]["restart"] for pid in pids]
    μ_restart = mean(restart)
    σ_restart = sqrt(var(restart))

    cumulative = [Schedulers._timers["map"][pid]["cumulative"] for pid in pids]
    μ_cumulative = mean(cumulative)
    σ_cumulative = sqrt(var(cumulative))

    uptime = [Schedulers._timers["map"][pid]["uptime"] for pid in pids]
    μ_uptime = mean(uptime)
    σ_uptime = sqrt(var(uptime))

    utilization = (f .+ checkpoint .+ restart) ./ cumulative
    μ_utilization = mean(utilization)
    σ_utilization = sqrt(var(utilization))

    utilization_f = f ./ cumulative
    μ_utilization_f = mean(utilization_f)
    σ_utilization_f = sqrt(var(utilization_f))

    x = """
    | pid      | f    | checkpoint    | restart    | cumulative    | uptime         | utilization   | utilization_f    | 
    |----------|------|---------------|------------|---------------|----------------|---------------|------------------|
    """
    for (ipid,pid) in enumerate(pids)
        x *= """
        | $pid | $(@sprintf("%.2f",f[ipid])) | $(@sprintf("%.2f",checkpoint[ipid])) | $(@sprintf("%.2f",restart[ipid])) | $(@sprintf("%.2f",cumulative[ipid])) | $(@sprintf("%.2f",uptime[ipid])) | $(@sprintf("%.2f",utilization[ipid])) | $(@sprintf("%.2f",utilization_f[ipid])) |
        """
    end
    x *= """
    | **mean**     | $(@sprintf("%.2f",μ_f)) | $(@sprintf("%.2f",μ_checkpoint)) | $(@sprintf("%.2f",μ_restart)) | $(@sprintf("%.2f",μ_cumulative)) | $(@sprintf("%.2f",μ_uptime)) | $(@sprintf("%.2f",μ_utilization)) | $(@sprintf("%.2f",μ_utilization_f)) |
    | **variance** | $(@sprintf("%.2f",σ_f)) | $(@sprintf("%.2f",σ_checkpoint)) | $(@sprintf("%.2f",σ_restart)) | $(@sprintf("%.2f",σ_cumulative)) | $(@sprintf("%.2f",σ_uptime)) | $(@sprintf("%.2f",σ_utilization)) | $(@sprintf("%.2f",σ_utilization_f)) |
    """
    write(filename, x)
end

function timers_analysis_reduce(filename)
    pids = collect(keys(Schedulers._timers["reduce"]))

    reduce = [Schedulers._timers["reduce"][pid]["reduce"] for pid in pids]
    μ_reduce = mean(reduce)
    σ_reduce = sqrt(var(reduce))

    io = [Schedulers._timers["reduce"][pid]["IO"] for pid in pids]
    μ_io = mean(io)
    σ_io = sqrt(var(io))

    cleanup = [Schedulers._timers["reduce"][pid]["cleanup"] for pid in pids]
    μ_cleanup = mean(cleanup)
    σ_cleanup = sqrt(var(cleanup))

    cumulative = [Schedulers._timers["reduce"][pid]["cumulative"] for pid in pids]
    μ_cumulative = mean(cumulative)
    σ_cumulative = sqrt(var(cumulative))

    uptime = [Schedulers._timers["reduce"][pid]["uptime"] for pid in pids]
    μ_uptime = mean(uptime)
    σ_uptime = sqrt(var(uptime))

    utilization = (reduce .+ io .+ cleanup) ./ cumulative
    μ_utilization = mean(utilization)
    σ_utilization = sqrt(var(utilization))

    utilization_reduce = reduce ./ cumulative
    μ_utilization_reduce = mean(utilization_reduce)
    σ_utilization_reduce = sqrt(var(utilization_reduce))

    x = """
    | pid      | reduce | IO | cleanup | cumulative | uptime | utilization   | utilization_reduce |
    |----------|--------|----|---------|------------|--------|---------------|--------------------|
    """
    for (ipid,pid) in enumerate(pids)
        x *= """
        | $pid | $(@sprintf("%.2f",reduce[ipid])) | $(@sprintf("%.2f",io[ipid])) | $(@sprintf("%.2f",cleanup[ipid])) | $(@sprintf("%.2f",cumulative[ipid])) | $(@sprintf("%.2f",uptime[ipid])) | $(@sprintf("%.2f",utilization[ipid])) | $(@sprintf("%.2f",utilization_reduce[ipid])) |
        """
    end
    x *= """
    | **mean**     | $(@sprintf("%.2f",μ_reduce)) | $(@sprintf("%.2f",μ_io)) | $(@sprintf("%.2f",μ_cleanup)) | $(@sprintf("%.2f",μ_cumulative)) | $(@sprintf("%.2f",μ_uptime)) | $(@sprintf("%.2f",μ_utilization)) | $(@sprintf("%.2f",μ_utilization_reduce)) |
    | **variance** | $(@sprintf("%.2f",σ_reduce)) | $(@sprintf("%.2f",σ_io)) | $(@sprintf("%.2f",σ_cleanup)) | $(@sprintf("%.2f",σ_cumulative)) | $(@sprintf("%.2f",σ_uptime)) | $(@sprintf("%.2f",σ_utilization)) | $(@sprintf("%.2f",σ_utilization_reduce)) |
    """
    write(filename, x)
end

let CID::Int = 1
    global next_checkpoint_id
    next_checkpoint_id() = (id = CID; CID += 1; id)
end
next_checkpoint(id, scratch) = joinpath(scratch, string("checkpoint-", id, "-", next_checkpoint_id()))

function reduce(reducer!, checkpoint1, checkpoint2, checkpoint3, ::Type{T}) where {T}
    t_io = @elapsed begin
        c1 = deserialize(checkpoint1)::T
        c2 = deserialize(checkpoint2)::T
    end
    t_sum = @elapsed begin
        reducer!(c2, c1)
    end
    t_io += @elapsed serialize(checkpoint3, c2)
    t_io, t_sum
end

save_checkpoint(checkpoint, localresult, ::Type{T}) where {T} = (serialize(checkpoint, fetch(localresult)::T); nothing)
restart(reducer!, orphan, localresult, ::Type{T}) where {T} = (reducer!(fetch(localresult)::T, deserialize(orphan)::T); nothing)
rm_checkpoint(checkpoint) = isfile(checkpoint) && rm(checkpoint)

function handle_process_exited(pid, localresults, checkpoints, orphans)
    rmprocs(pid)
    pop!(localresults, pid)
    checkpoint = pop!(checkpoints, pid)
    checkpoint == nothing || push!(orphans, checkpoint)
    nothing
end

export epmap, epmapreduce!

end
