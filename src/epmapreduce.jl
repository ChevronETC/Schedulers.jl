"""
Elastic parallel map-reduce functionality with fault tolerance.
"""

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
* `null_tsk_runtime_threshold=0` the maximum duration (in seconds) for a task to be considered insignificant or 'null' and thus not included in the timeout measurement.
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
function epmapreduce!(
    result::T,
    options::SchedulerOptions,
    f::Function,
    tasks,
    args...;
    kwargs...,
) where {T}
    options_init = copy(options)

    local epmap_eloop
    try
        for scratch in options.scratch
            isdir(scratch) || mkpath(scratch)
        end

        if options.zeros() === nothing
            options.zeros = ()->zeros(eltype(result), size(result))::T
        end

        epmap_journal = journal_init(tasks, options.journal_init_callback; reduce = true)

        epmap_eloop = ElasticLoop(
            typeof(next_checkpoint(options.id, options.scratch)),
            tasks,
            options;
            isreduce = true,
        )

        tsk_map = @async epmapreduce_map(
            f,
            result,
            epmap_eloop,
            epmap_journal,
            options,
            args...;
            kwargs...,
        )

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

epmapreduce!(result, f::Function, tasks, args...; kwargs...) =
    epmapreduce!(result, SchedulerOptions(), f, tasks, args...; kwargs...)

function epmapreduce_fetch_apply(
    _localresult,
    ::Type{T},
    epmapreduce_fetch,
    f,
    itsk,
    args...;
    kwargs...,
) where {T}
    localresult = epmapreduce_fetch(_localresult)::T
    f(localresult, itsk, args...; kwargs...)
    nothing
end

function epmapreduce_map(
    f,
    results::T,
    epmap_eloop,
    epmap_journal,
    options,
    args...;
    kwargs...,
) where {T}
    localresults = Dict{Int,Future}()

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
            put!(epmap_eloop.pid_channel_map_remove, (pid, false))
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
                        hostname = remotecall_fetch_timeout(
                            60,
                            1,
                            1,
                            nothing,
                            tsk->nothing,
                            tsk->nothing,
                            0,
                            options.gethostname,
                            pid,
                        )
                    catch e
                        @warn "unable to determine hostname for pid=$pid within 60 seconds."
                        logerror(e, Logging.Warn)
                        if epmap_eloop.checkpoints[pid] !== nothing
                            push!(
                                epmap_eloop.reduce_checkpoints,
                                epmap_eloop.checkpoints[pid],
                            )
                        end
                        pop!(localresults, pid)
                        pop!(epmap_eloop.checkpoints, pid)
                        put!(epmap_eloop.pid_channel_reduce_remove, (pid, true))
                        @debug "...finished cleanup for hostname failure for pid=$pid."
                        break
                    end
                end

                @debug "map, pid=$pid, interrupted=$(epmap_eloop.interrupted), isempty(epmap_eloop.tsk_pool_todo)=$(isempty(epmap_eloop.tsk_pool_todo))"
                @debug "epmap_eloop.is_reduce_triggered=$(epmap_eloop.is_reduce_triggered)"
                if isempty(epmap_eloop.tsk_pool_todo) ||
                   epmap_eloop.interrupted ||
                   (epmap_eloop.is_reduce_triggered && !epmap_eloop.checkpoints_are_flushed)
                    if epmap_eloop.checkpoints[pid] !== nothing
                        push!(epmap_eloop.reduce_checkpoints, epmap_eloop.checkpoints[pid])
                    end
                    pop!(localresults, pid)
                    pop!(epmap_eloop.checkpoints, pid)
                    put!(epmap_eloop.pid_channel_map_remove, (pid, false))
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
                    options.reporttasks &&
                        @info "running task $tsk on process $pid ($hostname); $(nworkers()) workers total; $(length(epmap_eloop.tsk_pool_todo)) tasks left in task-pool."
                    yield()
                    journal_start!(
                        epmap_journal,
                        options.journal_task_callback;
                        stage = "tasks",
                        tsk,
                        pid,
                        hostname,
                    )
                    remotecall_func_wait_timeout(
                        tsk_times,
                        epmap_eloop,
                        options,
                        preempt_channel_future,
                        options.checkpoint_task,
                        options.restart_task,
                        tsk,
                        epmapreduce_fetch_apply,
                        pid,
                        localresults[pid],
                        T,
                        options.epmapreduce_fetch,
                        f,
                        tsk,
                        args...;
                        kwargs...,
                    )
                    journal_stop!(
                        epmap_journal,
                        options.journal_task_callback;
                        stage = "tasks",
                        tsk,
                        pid,
                        fault = false,
                    )
                    @debug "...pid=$pid ($hostname),tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(epmap_eloop.tsk_pool_todo), tsk_pool_done=$(epmap_eloop.tsk_pool_done) -!"
                catch e
                    @warn "pid=$pid ($hostname), task loop, caught exception during f eval"
                    journal_stop!(
                        epmap_journal,
                        options.journal_task_callback;
                        stage = "tasks",
                        tsk,
                        pid,
                        fault = false,
                    )
                    if isa(e, TimeoutException) && options.skip_tasks_that_timeout
                        @warn "skipping task '$tsk' that timed out, compute/reduce step"
                        push!(epmap_eloop.tsk_pool_done, tsk)
                        push!(epmap_eloop.tsk_pool_timed_out, tsk)
                    else
                        push!(epmap_eloop.tsk_pool_todo, tsk)
                    end
                    r = handle_exception(
                        e,
                        pid,
                        hostname,
                        epmap_eloop.pid_failures,
                        options.maxerrors,
                        options.retries,
                    )
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    if r.do_break || r.do_interrupt
                        if epmap_eloop.checkpoints[pid] !== nothing
                            push!(
                                epmap_eloop.reduce_checkpoints,
                                epmap_eloop.checkpoints[pid],
                            )
                        end
                        pop!(localresults, pid)
                        pop!(epmap_eloop.checkpoints, pid)
                        put!(epmap_eloop.pid_channel_map_remove, (pid, r.bad_pid))
                        break
                    end
                    continue # no need to checkpoint since the task failed and will be re-run (TODO: or abandoned)
                end

                # checkpoint
                _next_checkpoint = next_checkpoint(options.id, options.scratch)
                try
                    @debug "running checkpoint for task $tsk on process $pid; $(nworkers()) workers total; $(length(epmap_eloop.tsk_pool_todo)) tasks left in task-pool."
                    journal_start!(epmap_journal; stage = "checkpoints", tsk, pid, hostname)
                    remotecall_wait_timeout(
                        checkpoint_times,
                        epmap_eloop.tsk_count,
                        options.timeout_multiplier,
                        nothing,
                        tsk->nothing,
                        tsk->nothing,
                        0,
                        save_checkpoint,
                        pid,
                        options.save_checkpoint,
                        options.epmapreduce_fetch,
                        _next_checkpoint,
                        localresults[pid],
                        T,
                    )
                    journal_stop!(
                        epmap_journal;
                        stage = "checkpoints",
                        tsk,
                        pid,
                        fault = false,
                    )
                    @debug "... checkpoint, pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(epmap_eloop.tsk_pool_todo) -!"
                    push!(epmap_eloop.tsk_pool_done, tsk)
                catch e
                    @warn "pid=$pid ($hostname), checkpoint=$(epmap_eloop.checkpoints[pid]), task loop, caught exception during save_checkpoint"
                    journal_stop!(
                        epmap_journal;
                        stage = "checkpoints",
                        tsk,
                        pid,
                        fault = true,
                    )
                    @debug "pushing task onto tsk_pool_todo list"
                    if isa(e, TimeoutException) && options.skip_tasks_that_timeout
                        @warn "skipping task '$tsk' that timed out, checkpoint step"
                        push!(epmap_eloop.tsk_pool_done, tsk)
                        push!(epmap_eloop.tsk_pool_timed_out, tsk)
                    else
                        push!(epmap_eloop.tsk_pool_todo, tsk)
                    end
                    @debug "handling exception"
                    r = handle_exception(
                        e,
                        pid,
                        hostname,
                        epmap_eloop.pid_failures,
                        options.maxerrors,
                        options.retries,
                    )
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
                            push!(
                                epmap_eloop.reduce_checkpoints,
                                epmap_eloop.checkpoints[pid],
                            )
                        end
                        @debug "popping localresults"
                        pop!(localresults, pid)
                        @debug "popping checkpoints"
                        pop!(epmap_eloop.checkpoints, pid)
                        @debug "returning pid to elastic loop"
                        put!(epmap_eloop.pid_channel_map_remove, (pid, r.bad_pid))
                        @debug "done returning pid to elastic loop"
                        break
                    end

                    # populate local reduction with the previous checkpoint since we will re-run the task.
                    if epmap_eloop.checkpoints[pid] === nothing
                        @debug "re-zeroing local result (no existing checkpoints)"
                        localresults[pid] =
                            remotecall_default_threadpool(options.zeros, pid)
                    else
                        @debug "re-setting localresult to the previously saved checkpoint"
                        localresults[pid] = remotecall_default_threadpool(
                            load_checkpoint,
                            pid,
                            options.load_checkpoint,
                            epmap_eloop.checkpoints[pid],
                            T,
                        )
                    end

                    continue # there is no new checkpoint, and we need to keep the old checkpoint
                end

                # delete old checkpoint
                old_checkpoint, epmap_eloop.checkpoints[pid] =
                    get(epmap_eloop.checkpoints, pid, nothing), _next_checkpoint
                try
                    if old_checkpoint !== nothing
                        @debug "deleting old checkpoint"
                        journal_start!(
                            epmap_journal;
                            stage = "rmcheckpoints",
                            tsk,
                            pid,
                            hostname,
                        )
                        options.keepcheckpoints || remotecall_wait_timeout(
                            rm_times,
                            epmap_eloop.tsk_count,
                            options.timeout_multiplier,
                            nothing,
                            tsk->nothing,
                            tsk->nothing,
                            0,
                            options.rm_checkpoint,
                            pid,
                            old_checkpoint,
                        )
                        journal_stop!(
                            epmap_journal;
                            stage = "rmcheckpoint",
                            tsk,
                            pid,
                            fault = false,
                        )
                    end
                catch e
                    @warn "pid=$pid ($hostname), task loop, caught exception during remove checkpoint, there may be stray check-point files that will be deleted later"
                    push!(checkpoint_orphans, old_checkpoint)
                    journal_stop!(
                        epmap_journal;
                        stage = "checkpoints",
                        tsk,
                        pid,
                        fault = true,
                    )
                    r = handle_exception(
                        e,
                        pid,
                        hostname,
                        epmap_eloop.pid_failures,
                        options.maxerrors,
                        options.retries,
                    )
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    if r.do_break || r.do_interrupt
                        if epmap_eloop.checkpoints[pid] !== nothing
                            push!(
                                epmap_eloop.reduce_checkpoints,
                                epmap_eloop.checkpoints[pid],
                            )
                        end
                        pop!(localresults, pid)
                        pop!(epmap_eloop.checkpoints, pid)
                        put!(epmap_eloop.pid_channel_map_remove, (pid, r.bad_pid))
                        break
                    end
                end
            end
        catch e
            @warn "map, uncaught exception in worker loop for pid=$pid"
            logerror(e, Logging.Warn)
            @debug "map, putting $pid onto remove channel"
            isopen(epmap_eloop.pid_channel_map_remove) &&
                put!(epmap_eloop.pid_channel_map_remove, (pid, true))
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
            hostname = remotecall_fetch_timeout(
                60,
                1,
                1,
                nothing,
                tsk->nothing,
                tsk->nothing,
                0,
                options.gethostname,
                pid,
            )
        catch e
            @warn "unable to determine host name for pid=$pid"
            logerror(e, Logging.Warn)
            put!(epmap_eloop.pid_channel_reduce_remove, (pid, true))
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

                n_checkpoints =
                    epmap_eloop.is_reduce_triggered ?
                    length(epmap_eloop.reduce_checkpoints_snapshot) :
                    length(epmap_eloop.reduce_checkpoints)

                @debug "reduce, pid=$pid, at exit condition, n_checkpoints=$n_checkpoints"
                if n_checkpoints < 2
                    unlock(l)
                    @debug "reduce, popping pid=$pid from dirty list"
                    pop!(epmap_eloop.reduce_checkpoints_is_dirty, pid)
                    @debug "reduce, putting $pid onto reduce remove channel"
                    put!(epmap_eloop.pid_channel_reduce_remove, (pid, false))
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
                            icheckpoint1 = findfirst(
                                checkpoint->checkpoint == checkpoint1,
                                epmap_eloop.reduce_checkpoints,
                            )
                            icheckpoint1 === nothing ||
                                deleteat!(epmap_eloop.reduce_checkpoints, icheckpoint1)
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
                            icheckpoint2 = findfirst(
                                checkpoint->checkpoint == checkpoint2,
                                epmap_eloop.reduce_checkpoints,
                            )
                            icheckpoint2 === nothing ||
                                deleteat!(epmap_eloop.reduce_checkpoints, icheckpoint2)
                        else
                            checkpoint2 = popfirst!(epmap_eloop.reduce_checkpoints)
                        end
                        @debug "reduce, popped second checkpoint, pid=$pid" checkpoint2
                    catch e
                        @warn "reduce, unable to pop checkpoint 2"
                        logerror(e, Logging.Warn)
                        push!(epmap_eloop.reduce_checkpoints, checkpoint1)
                        epmap_eloop.is_reduce_triggered &&
                            push!(epmap_eloop.reduce_checkpoints_snapshot, checkpoint1)
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
                        epmap_eloop.is_reduce_triggered && push!(
                            epmap_eloop.reduce_checkpoints_snapshot,
                            checkpoint1,
                            checkpoint2,
                        )
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
                    journal_start!(epmap_journal; stage = "reduce", tsk = 0, pid, hostname)
                    # We don't have a good way for estimating the number of reduction tasks (due to the dynamic nature of the resources), so we choose an arbibrary number (10).
                    remotecall_wait_timeout(
                        reduce_times,
                        10,
                        options.timeout_multiplier,
                        nothing,
                        tsk->nothing,
                        tsk->nothing,
                        0,
                        reduce,
                        pid,
                        options.reducer!,
                        options.save_checkpoint,
                        options.epmapreduce_fetch,
                        options.load_checkpoint,
                        checkpoint1,
                        checkpoint2,
                        checkpoint3,
                        T,
                    )
                    journal_stop!(
                        epmap_journal;
                        stage = "reduce",
                        tsk = 0,
                        pid,
                        fault = false,
                    )
                    push!(epmap_eloop.reduce_checkpoints, checkpoint3)
                    epmap_eloop.is_reduce_triggered &&
                        push!(epmap_eloop.reduce_checkpoints_snapshot, checkpoint3)
                    @debug "pushed reduced checkpoint3, pid=$pid" checkpoint3
                catch e
                    push!(epmap_eloop.reduce_checkpoints, checkpoint1, checkpoint2)
                    epmap_eloop.is_reduce_triggered && push!(
                        epmap_eloop.reduce_checkpoints_snapshot,
                        checkpoint1,
                        checkpoint2,
                    )
                    @warn "pid=$pid ($hostname), reduce loop, caught exception during reduce"
                    r = handle_exception(
                        e,
                        pid,
                        hostname,
                        epmap_eloop.pid_failures,
                        options.maxerrors,
                        options.retries,
                    )
                    journal_stop!(
                        epmap_journal;
                        stage = "reduce",
                        tsk = 0,
                        pid,
                        fault = true,
                    )
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    if r.do_break || r.do_interrupt
                        pop!(epmap_eloop.reduce_checkpoints_is_dirty, pid)
                        put!(epmap_eloop.pid_channel_reduce_remove, (pid, r.bad_pid))
                        epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                        break
                    end
                    epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                    continue
                end

                # remove checkpoint 1
                try
                    options.keepcheckpoints ||
                        @debug "removing checkpoint 1, pid=$pid" checkpoint1
                    journal_start!(epmap_journal; stage = "reduce", tsk = 0, pid, hostname)
                    # We don't have a good way for estimating the number of deletion tasks (due to the dynamic nature of the resources), so we choose an arbibrary number (10).
                    options.keepcheckpoints || remotecall_wait_timeout(
                        rm_times,
                        10,
                        options.timeout_multiplier,
                        nothing,
                        tsk->nothing,
                        tsk->nothing,
                        0,
                        options.rm_checkpoint,
                        pid,
                        checkpoint1,
                    )
                    journal_stop!(
                        epmap_journal;
                        stage = "reduce",
                        tsk = 0,
                        pid,
                        fault = false,
                    )
                    options.keepcheckpoints ||
                        @debug "removed checkpoint 1, pid=$pid" checkpoint1
                catch e
                    @warn "pid=$pid ($hostname), reduce loop, caught exception during remove checkpoint 1"
                    r = handle_exception(
                        e,
                        pid,
                        hostname,
                        epmap_eloop.pid_failures,
                        options.maxerrors,
                        options.retries,
                    )
                    journal_stop!(
                        epmap_journal;
                        stage = "reduce",
                        tsk = 0,
                        pid,
                        fault = true,
                    )
                    push!(orphans_remove, checkpoint1, checkpoint2)
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    if r.do_break || r.do_interrupt
                        pop!(epmap_eloop.reduce_checkpoints_is_dirty, pid)
                        put!(epmap_eloop.pid_channel_reduce_remove, (pid, r.bad_pid))
                        break
                    end
                    epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                    continue
                end

                # remove checkpoint 2
                try
                    options.keepcheckpoints ||
                        @debug "removing checkpoint 2, pid=$pid" checkpoint2
                    journal_start!(epmap_journal; stage = "reduce", tsk = 0, pid, hostname)
                    # We don't have a good way for estimating the number of deletion tasks (due to the dynamic nature of the resources), so we choose an arbibrary number (10).
                    options.keepcheckpoints || remotecall_wait_timeout(
                        rm_times,
                        10,
                        options.timeout_multiplier,
                        nothing,
                        tsk->nothing,
                        tsk->nothing,
                        0,
                        options.rm_checkpoint,
                        pid,
                        checkpoint2,
                    )
                    journal_stop!(
                        epmap_journal;
                        stage = "reduce",
                        tsk = 0,
                        pid,
                        fault = false,
                    )
                    options.keepcheckpoints ||
                        @debug "removed checkpoint 2, pid=$pid" checkpoint2
                catch e
                    @warn "pid=$pid ($hostname), reduce loop, caught exception during remove checkpoint 2"
                    r = handle_exception(
                        e,
                        pid,
                        hostname,
                        epmap_eloop.pid_failures,
                        options.maxerrors,
                        options.retries,
                    )
                    journal_stop!(
                        epmap_journal;
                        stage = "reduce",
                        tsk = 0,
                        pid,
                        fault = true,
                    )
                    push!(orphans_remove, checkpoint2)
                    epmap_eloop.interrupted = r.do_interrupt
                    epmap_eloop.errored = r.do_error
                    if r.do_break || r.do_interrupt
                        pop!(epmap_eloop.reduce_checkpoints_is_dirty, pid)
                        put!(epmap_eloop.pid_channel_reduce_remove, (pid, r.bad_pid))
                        break
                    end
                end

                epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
            end
        catch e
            @warn "reduce, uncaught exception in worker loop for pid=$pid"
            logerror(e, Logging.Warn)
            @debug "reduce, putting $pid onto remove channel"
            isopen(epmap_eloop.pid_channel_reduce_remove) &&
                put!(epmap_eloop.pid_channel_reduce_remove, (pid, true))
            @debug "reduce, done putting $pid onto remove channel"
        end
    end
    @debug "reduce, exited the reduce worker loop"

    length(epmap_eloop.reduce_checkpoints) == 0 &&
        @warn "there are no checkpoints to reduce indicating that no tasks were run"

    for i = 1:10
        try
            if length(epmap_eloop.reduce_checkpoints) > 0
                options.reducer!(
                    result,
                    load_checkpoint(
                        options.load_checkpoint,
                        epmap_eloop.reduce_checkpoints[1],
                        T,
                    ),
                )
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
