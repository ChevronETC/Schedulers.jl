module Schedulers

using Distributed, DistributedOperations, Printf, Random, Statistics

epmap_default_addprocs = n->addprocs(n)
epmap_default_preempted = ()->false

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

function elastic_loop(pid_channel, rm_pid_channel, tsk_pool_done, tsk_pool_todo, tsk_count, interrupted, epmap_minworkers, epmap_maxworkers, epmap_quantum, epmap_addprocs)
    for worker in workers()
        _pid_up_timestamp[worker] = time()
        put!(pid_channel, worker)
    end

    while true
        @debug "checking pool, length=$(length(tsk_pool_done)), count=$tsk_count"
        yield()
        length(tsk_pool_done) == tsk_count && (put!(pid_channel, -1); break)
        @debug "checking for interrupt=$interrupted"
        yield()
        interrupted && break
        @debug "checking for workers, nworkers=$(nworkers()), max=$epmap_maxworkers, #todo=$(length(tsk_pool_todo))"
        yield()
        n = min(epmap_maxworkers-nworkers(), epmap_quantum, length(tsk_pool_todo))
        if n > 0
            new_pids = epmap_addprocs(n)
            for new_pid in new_pids
                load_modules_on_new_workers(new_pid)
                load_functions_on_new_workers(new_pid)
            end
            for new_pid in new_pids
                _pid_up_timestamp[new_pid] = time()
                put!(pid_channel, new_pid)
            end
        end

        @debug "checking for workers to remove"
        while isready(rm_pid_channel)
            pid = take!(rm_pid_channel)
            _nworkers = 1 ∈ workers() ? nworkers()-1 : nworkers()
            @debug "removing worker $pid"
            if _nworkers > epmap_minworkers
                rmprocs(pid)
            end
        end

        sleep(5)
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
* `epmap_minworkers=nworkers()` the minimum number of workers to elastically shrink to
* `epmap_maxworkers=nworkers()` the maximum number of workers to elastically expand to
* `epmap_quantum=32` the maximum number of workers to elastically add at a time
* `epmap_addprocs=n->addprocs(n)` method for adding n processes (will depend on the cluster manager being used)
* `epmap_preempted=()->false` method for determining of a machine got pre-empted (removed on purpose)[1]

## Notes
[1] For example, on Azure Cloud a SPOT instance will be pre-empted if someone is willing to pay more for it
"""
function epmap(f::Function, tasks, args...;
        epmap_retries = 0,
        epmap_maxerrors = Inf,
        epmap_minworkers = nworkers(),
        epmap_maxworkers = nworkers(),
        epmap_quantum = 32,
        epmap_addprocs = epmap_default_addprocs,
        epmap_preempted = epmap_default_preempted,
        kwargs...)
    tsk_pool_todo = collect(tasks)
    tsk_pool_done = []
    tsk_count = length(tsk_pool_todo)

    pid_channel = Channel{Int}(32)
    rm_pid_channel = Channel{Int}(32)

    fails = Dict{Int,Int}()
    map(pid->fails[pid]=0, workers())

    interrupted = false

    _elastic_loop = @async elastic_loop(pid_channel, rm_pid_channel, tsk_pool_done, tsk_pool_todo, tsk_count, interrupted, epmap_minworkers, epmap_maxworkers, epmap_quantum, epmap_addprocs)

    # work loop
    @sync while true
        interrupted && break
        pid = take!(pid_channel)
        @debug "pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.
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
                @info "running task $tsk on process $pid; $(nworkers()) workers total; $(length(tsk_pool_todo)) tasks left in task-pool."
                yield()
                remotecall_fetch(f, pid, tsk, args...; kwargs...)
                @debug "...pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$tsk_pool_todo -!"
                yield()
                push!(tsk_pool_done, tsk)
            catch e
                fails[pid] += 1
                nerrors = sum(values(fails))
                @warn "caught an exception, there have been $(fails[pid]) failure(s) on process $pid..."
                showerror(stderr, e)
                push!(tsk_pool_todo, tsk)
                if isa(e, InterruptException)
                    interrupted = true
                    put!(pid_channel, -1)
                    throw(e)
                elseif isa(e, ProcessExitedException)
                    @warn "process with id=$pid exited, removing from process list"
                    rmprocs(pid)
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
    rmprocs(_workers[1:(length(_workers) - epmap_minworkers)])

    nothing
end

"""
    epmapreduce!(result, f, tasks, args...; epmap_kwargs..., kwargs...) -> result

where f is the map function, and `tasks` are an iterable set of tasks to map over.  The
positional arguments `args` and the named arguments `kwargs` are passed to `f` which has
the method signature: `f(localresult, f, task, args; kwargs...)`.  `localresult` is a Future
with an assoicated partial reduction.

# pmap_kwargs
* `epmap_minworkers=nworkers()` the minimum number of workers to elastically shrink to
* `epmap_maxworkers=nworkers()` the maximum number of workers to elastically expand to
* `epmap_quantum=32` the maximum number of workers to elastically add at a time
* `epmap_addprocs=n->addprocs(n)` method for adding n processes (will depend on the cluster manager being used)
* `epmap_preempted=()->false` method for determining of a machine got pre-empted (removed on purpose)[1]

# Example
```julia
using Distributed
addprocs(2)
@everywhere using Distributed, Schedulers
@everywhere f(x, tsk) = (fetch(x)::Vector{Float32} .+= tsk; nothing)
result = epmapreduce(zeros(Float32,10), f, 1:100)
rmprocs(workers())
```
"""
function epmapreduce!(result::AbstractArray{T,N}, f, tasks, args...;
        epmapreduce_id = randstring(6),
        epmap_minworkers = nworkers(),
        epmap_maxworkers = nworkers(),
        epmap_quantum = 32,
        epmap_addprocs = epmap_default_addprocs,
        epmap_scratch = "/scratch",
        kwargs...) where {T,N}
    isdir(epmap_scratch) || mkpath(epmap_scratch)
    empty!(_timers)
    checkpoints = epmapreduce_map(f, tasks, T, size(result), args...;
        epmapreduce_id=epmapreduce_id, epmap_minworkers=epmap_minworkers, epmap_maxworkers=epmap_maxworkers, epmap_quantum=epmap_quantum, epmap_addprocs=epmap_addprocs, epmap_scratch=epmap_scratch, kwargs...)
    epmapreduce_reduce!(result, checkpoints;
        epmapreduce_id=epmapreduce_id, epmap_minworkers=epmap_minworkers, epmap_maxworkers=epmap_maxworkers, epmap_quantum=epmap_quantum, epmap_addprocs=epmap_addprocs, epmap_scratch=epmap_scratch)
end

function epmapreduce_map(f, tasks, ::Type{T}, n::NTuple{N,Int}, args...;
        epmapreduce_id,
        epmap_minworkers,
        epmap_maxworkers,
        epmap_quantum,
        epmap_addprocs,
        epmap_scratch,
        kwargs...) where {T,N}
    tsk_pool_todo = collect(tasks)
    tsk_pool_done = []
    interrupted = false
    tsk_count = length(tsk_pool_todo)

    pid_channel = Channel{Int}(32)
    rm_pid_channel = Channel{Int}(32)

    _elastic_loop = @async elastic_loop(pid_channel, rm_pid_channel, tsk_pool_done, tsk_pool_todo, tsk_count, interrupted, epmap_minworkers, epmap_maxworkers, epmap_quantum, epmap_addprocs)

    localresults = Dict{Int, Future}()
    checkpoints = Dict{Int, Any}()
    orphans_compute = Set{Any}()
    orphans_remove = Set{Any}()

    _timers["map"] = Dict{Int, Dict{String,Float64}}()
    tic_cumulative = Dict{Int,Float64}()

    # task loop
    @info "task loop..."
    @sync while true
        pid = take!(pid_channel)
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.

        localresults[pid] = remotecall(zeros, pid, T, n)
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
                    _timers["map"][pid]["restart"] += @elapsed remotecall_fetch(restart, pid, orphan, localresults[pid], T, n)
                catch e
                    @warn "caught restart error, reduce-in orhpan checkpoing"
                    push!(orphans_compute, orphan)
                    isa(e, ProcessExitedException) && (rmprocs(pid); break)
                    error("TODO")
                    throw(e)
                end
                
                try
                    _next_checkpoint = next_checkpoint(epmapreduce_id, epmap_scratch)
                    _timers["map"][pid]["restart"] += @elapsed remotecall_fetch(save_checkpoint, pid, _next_checkpoint, localresults[pid], T, n)
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
                @info "running task $tsk on process $pid; $(nworkers()) workers total; $(length(tsk_pool_todo)) tasks left in task-pool."
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
                _timers["map"][pid]["checkpoint"] += @elapsed remotecall_fetch(save_checkpoint, pid, _next_checkpoint, localresults[pid], T, n)
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
    @info "...done task loop"
    filter!(checkpoint->checkpoint != nothing, [collect(values(checkpoints)); orphans_compute...])
end

function epmapreduce_reduce!(result::AbstractArray{T,N}, checkpoints;
        epmapreduce_id,
        epmap_minworkers,
        epmap_maxworkers,
        epmap_quantum,
        epmap_addprocs,
        epmap_scratch) where {T,N}
    @info "reduce loop..."
    n_checkpoints = length(checkpoints)
    # reduce loop, tsk_pool_todo and tsk_pool_done are not really needed, but lets us reuse the elastic_loop method
    tsk_pool_todo = [1:n_checkpoints-1;]
    tsk_pool_done = Int[]
    ntsks = length(tsk_pool_todo)
    interrupted = false

    pid_channel = Channel{Int}(32)
    rm_pid_channel = Channel{Int}(32)
    
    _elastic_loop = @async elastic_loop(pid_channel, rm_pid_channel, tsk_pool_done, tsk_pool_todo, ntsks, interrupted, epmap_minworkers, epmap_maxworkers, epmap_quantum, epmap_addprocs)

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
                t_io, t_sum = remotecall_fetch(reduce, pid, checkpoint1, checkpoint2, checkpoint3, T, size(result))
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
    @info "...done reduce loop."

    # ensure we are left with epmap_minworkers
    _workers = workers()
    1 ∈ _workers && popfirst!(_workers)
    rmprocs(_workers[1:(length(_workers) - epmap_minworkers)])

    x = read!(checkpoints[1], result)
    rm_checkpoint(checkpoints[1])
    rm_checkpoint.(orphans_remove)
    x
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

function reduce(checkpoint1, checkpoint2, checkpoint3, ::Type{T}, n::NTuple{N,Int}) where {T,N}
    t_io = @elapsed begin
        c1 = read!(checkpoint1, Array{T,N}(undef, n))
        c2 = read!(checkpoint2, Array{T,N}(undef, n))
    end
    t_sum = @elapsed begin
        c3 = c1 .+ c2
    end
    t_io += @elapsed write(checkpoint3, c3)
    t_io, t_sum
end

save_checkpoint(checkpoint, localresult, ::Type{T}, n::NTuple{N,Int}) where {T,N} = (write(checkpoint, fetch(localresult)::Array{T,N}); nothing)
restart(orphan, localresult, ::Type{T}, n::NTuple{N,Int}) where {T,N} = (fetch(localresult)::Array{T,N} .+= read!(orphan, Array{T,N}(undef, n)); nothing)
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
