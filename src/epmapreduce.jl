"""
    epmapreduce!(result, [options], f, tasks, args...; kwargs...) -> result

where `f` is the map function, and `tasks` are an iterable set of tasks to map over.  The
positional arguments `args` and the named arguments `kwargs` are passed to `f` which has
the method signature: `f(localresult, f, task, args; kwargs...)`.  `localresult` is
the associated partial reduction contribution to `result`.  `epmapreduce!` is parameterized
by `options::SchedulerOptions` and can be built using `options=SchedulerOptions(;epmap_kwargs...)`
and `epmap_kwargs` are as follows.

## epmap_kwargs
* `reducer! = default_reducer!` the method used to reduce the result. The default is `(x,y)->(x .+= y)`
* `save_checkpoint = default_save_checkpoint` the method used to save checkpoints[1]
* `load_checkpoint = default_load_checkpoint` the method used to load a checkpoint[2]
* `zeros = ()->zeros(eltype(result), size(result))` the method used to initialize partial reductions
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
* `scratch=["/scratch"]` storage location accessible to all cluster machines (e.g NFS, Azure blobstore,...)[4]
* `reporttasks=true` log task assignment
* `journalfile=""` write a journal showing what was computed where to a json file
* `journal_init_callback=tsks->nothing` additional method when initializing the journal
* `journal_task_callback=tsk->nothing` additional method when journaling a task
* `id=randstring(6)` identifier used for the scratch files
* `reduce_trigger=eloop->nothing` condition for triggering a reduction prior to the completion of the map
* `save_partial_reduction=x->nothing` method for saving a partial reduction triggered by `reduce_trigger`
## Notes
[1] The signature is `my_save_checkpoint(checkpoint_file, x)` where `checkpoint_file` is the file that will be
written to, and `x` is the data that will be written.
[2] The signature is `my_load_checkpoint(checkpoint_file)` where `checkpoint_file` is the file that
data will be loaded from.
[3] The number of machines provisioined may be greater than the number of workers in the cluster since with
some cluster managers, there may be a delay between the provisioining of a machine, and when it is added to the
Julia cluster.
[4] If more than one scratch location is selected, then check-point files will be distributed across those locations.
This can be useful if you are, for example, constrained by cloud storage through-put limits.

# Examples
## Example 1
With the assumption that `/scratch` is accessible from all workers:
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
            _et = eltype(result)
            _sz = size(result)
            options.zeros = ()->zeros(_et, _sz)::T
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
        @debug "map, interrupted=$(is_interrupted(epmap_eloop))"
        is_interrupted(epmap_eloop) && break
        pid = take!(epmap_eloop.pid_channel_map_add)

        @debug "map, pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.

        if pid ∈ keys(localresults) # task loop has already run for this pid
            put!(epmap_eloop.events, WorkerFreed(pid, false, :map))
            continue
        end

        hostname = ""

        set_worker_checkpoint!(epmap_eloop, pid, nothing)

        @debug "map, retrieving preempt channel future, pid=$pid"
        local preempt_channel_future
        try
            preempt_channel_future = options.preempt_channel_future(pid)
        catch e
            @warn "failed to retrieve preempt_channel_future, checkpoint/restart functionality disabled" pid
            logerror(e, Logging.Debug)
            preempt_channel_future = nothing
        end
        @debug "map, done retrieving preempt channel future, pid=$pid"

        @async try
            # This is an async remotecall.  Exceptions will be caught the first time we fetch `localresults[pid]` in the `epmapreduce_fetch_apply` method.
            # this is in the async block for the case where `options.zeros` sends a large amount of data to pid (should not be true in general)
            @debug "map, getting local result, pid=$pid"
            localresults[pid] = remotecall_default_threadpool(options.zeros, pid)
            @debug "map, done getting local result, pid=$pid"

            while true
                if hostname == ""
                    try
                        @debug "fetching hostname for pid=$pid with a timeout of 60 seconds"
                        hostname = remotecall_fetch_timeout(60, 1, 1, nothing, tsk->nothing, tsk->nothing, 0, options.gethostname, pid)
                        @debug "fetched hostname for pid=$pid: $hostname"
                    catch e
                        @warn "unable to determine hostname for pid=$pid within 60 seconds."
                        logerror(e, Logging.Debug)
                        flush_worker_checkpoint!(epmap_eloop, pid)
                        pop!(localresults, pid)
                        put!(epmap_eloop.events, WorkerFreed(pid, true, :reduce))
                        @debug "...finished cleanup for hostname failure for pid=$pid."
                        break
                    end
                end

                @debug "map, pid=$pid, interrupted=$(is_interrupted(epmap_eloop)), tasks_remaining=$(tasks_remaining(epmap_eloop))"
                @debug "epmap_eloop.is_reduce_triggered=$(epmap_eloop.is_reduce_triggered)"
                if tasks_remaining(epmap_eloop) == 0 || is_interrupted(epmap_eloop) || (epmap_eloop.is_reduce_triggered && !epmap_eloop.checkpoints_are_flushed)
                    flush_worker_checkpoint!(epmap_eloop, pid)
                    pop!(localresults, pid)
                    put!(epmap_eloop.events, WorkerFreed(pid, false, :map))
                    break
                end

                @debug "map, getting next task for pid=$pid"
                tsk = pop_next_task!(epmap_eloop)
                tsk === nothing && (yield(); continue)
                @debug "map, got next task for pid=$pid: $tsk"

                # compute and reduce
                try
                    options.reporttasks && @info "running task $tsk on process $pid ($hostname); $(nworkers()) julia workers total; $(options.nworkers()) provisioned workers total; $(tasks_remaining(epmap_eloop)) tasks left in task-pool."
                    yield()
                    journal_start!(epmap_journal, options.journal_task_callback; stage="tasks", tsk, pid, hostname)
                    remotecall_func_wait_timeout(tsk_times, epmap_eloop, options, preempt_channel_future, options.checkpoint_task, options.restart_task, tsk, epmapreduce_fetch_apply, pid, localresults[pid], T, options.epmapreduce_fetch, f, tsk, args...; kwargs...)
                    journal_stop!(epmap_journal, options.journal_task_callback; stage="tasks", tsk, pid, fault=false)
                    @debug "...pid=$pid ($hostname),tsk=$tsk,nworkers()=$(nworkers()),options.nworkers()=$(options.nworkers()), tasks_remaining=$(tasks_remaining(epmap_eloop)), tasks_done=$(tasks_done_count(epmap_eloop)) -!"
                catch e
                    @warn "task execution failed" pid hostname tsk failures=get_pid_failures(epmap_eloop, pid)
                    journal_stop!(epmap_journal, options.journal_task_callback; stage="tasks", tsk, pid, fault=true)
                    action = handle_exception(e, pid, hostname, epmap_eloop, options.maxerrors, options.retries)
                    actual_e = e isa TaskFailedException ? e.task.result : e
                    if isa(actual_e, TimeoutException) && options.skip_tasks_that_timeout
                        @warn "skipping task that timed out, compute/reduce step" tsk pid
                        mark_task_timed_out!(epmap_eloop, tsk)
                    elseif !action.retry_task
                        if !tsk_was_retried(epmap_eloop, tsk)
                            @warn "task failed, allowing one cross-worker retry" tsk pid
                            tsk_retried_add!(epmap_eloop, tsk)
                            requeue_task!(epmap_eloop, tsk)
                        else
                            @warn "task permanently failed after cross-worker retry" tsk pid
                            mark_task_timed_out!(epmap_eloop, tsk)
                        end
                    else
                        requeue_task!(epmap_eloop, tsk)
                    end
                    action.do_interrupt && set_interrupted!(epmap_eloop)
                    action.do_error && set_errored!(epmap_eloop)
                    if action.do_break || action.do_interrupt
                        flush_worker_checkpoint!(epmap_eloop, pid)
                        pop!(localresults, pid)
                        put!(epmap_eloop.events, WorkerFreed(pid, action.bad_pid, :map))
                        break
                    end
                    continue # no need to checkpoint since the task failed and will be re-run (TODO: or abandoned)
                end

                # checkpoint
                _next_checkpoint = next_checkpoint(options.id, options.scratch)
                try
                    @debug "running checkpoint for task $tsk on process $pid; $(nworkers()) workers total; $(tasks_remaining(epmap_eloop)) tasks left in task-pool."
                    journal_start!(epmap_journal; stage="checkpoints", tsk, pid, hostname)
                    remotecall_wait_timeout(checkpoint_times, epmap_eloop.tsk_count, options.timeout_multiplier, nothing, tsk->nothing, tsk->nothing, 0, save_checkpoint, pid, options.save_checkpoint, options.epmapreduce_fetch, _next_checkpoint, localresults[pid], T)
                    journal_stop!(epmap_journal; stage="checkpoints", tsk, pid, fault=false)
                    @debug "... checkpoint, pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tasks_remaining=$(tasks_remaining(epmap_eloop)) -!"
                    mark_task_done!(epmap_eloop, tsk)
                catch e
                    @warn "checkpoint save failed" pid hostname checkpoint=get_worker_checkpoint(epmap_eloop, pid) tsk
                    journal_stop!(epmap_journal; stage="checkpoints", tsk, pid, fault=true)
                    @debug "pushing task onto tsk_pool_todo list"
                    action = handle_exception(e, pid, hostname, epmap_eloop, options.maxerrors, options.retries)
                    actual_e = e isa TaskFailedException ? e.task.result : e
                    if isa(actual_e, TimeoutException) && options.skip_tasks_that_timeout
                        @warn "skipping task that timed out, checkpoint step" tsk pid
                        mark_task_timed_out!(epmap_eloop, tsk)
                    else
                        # Checkpoint failure is infrastructure, not task fault — always retry
                        requeue_task!(epmap_eloop, tsk)
                    end
                    @debug "handling exception"
                    action.do_interrupt && set_interrupted!(epmap_eloop)
                    action.do_error && set_errored!(epmap_eloop)
                    @debug "caught save checkpoint" action.do_break action.do_interrupt _next_checkpoint
                    # note that `options.rm_checkpoint` should check if the file exists before attempting removal
                    try
                        options.rm_checkpoint(_next_checkpoint)
                    catch e
                        @warn "unable to delete checkpoint, manual clean-up may be required" checkpoint=_next_checkpoint
                        logerror(e, Logging.Debug)
                    end
                    if action.do_break || action.do_interrupt
                        @debug "flushing worker checkpoint for pid=$pid"
                        flush_worker_checkpoint!(epmap_eloop, pid)
                        @debug "popping localresults"
                        pop!(localresults, pid)
                        @debug "returning pid to elastic loop"
                        put!(epmap_eloop.events, WorkerFreed(pid, action.bad_pid, :map))
                        @debug "done returning pid to elastic loop"
                        break
                    end

                    # populate local reduction with the previous checkpoint since we will re-run the task.
                    _prev_checkpoint = get_worker_checkpoint(epmap_eloop, pid)
                    if _prev_checkpoint === nothing
                        @debug "re-zeroing local result (no existing checkpoints)"
                        localresults[pid] = remotecall_default_threadpool(options.zeros, pid)
                    else
                        @debug "re-setting localresult to the previously saved checkpoint"
                        localresults[pid] = remotecall_default_threadpool(load_checkpoint, pid, options.load_checkpoint, _prev_checkpoint, T)
                    end

                    continue # there is no new checkpoint, and we need to keep the old checkpoint
                end

                # delete old checkpoint
                old_checkpoint = get_worker_checkpoint(epmap_eloop, pid)
                set_worker_checkpoint!(epmap_eloop, pid, _next_checkpoint)
                try
                    if old_checkpoint !== nothing
                        @debug "deleting old checkpoint"
                        journal_start!(epmap_journal; stage="rmcheckpoints", tsk, pid, hostname)
                        options.keepcheckpoints || remotecall_wait_timeout(rm_times, epmap_eloop.tsk_count, options.timeout_multiplier, nothing, tsk->nothing, tsk->nothing, 0, options.rm_checkpoint, pid, old_checkpoint)
                        journal_stop!(epmap_journal; stage="rmcheckpoint", tsk, pid, fault=false)
                    end
                catch e
                    @warn "checkpoint remove failed, stray check-point files will be deleted later" pid hostname checkpoint=old_checkpoint
                    push!(checkpoint_orphans, old_checkpoint)
                    journal_stop!(epmap_journal; stage="checkpoints", tsk, pid, fault=true)
                    action = handle_exception(e, pid, hostname, epmap_eloop, options.maxerrors, options.retries)
                    action.do_interrupt && set_interrupted!(epmap_eloop)
                    action.do_error && set_errored!(epmap_eloop)
                    if action.do_break || action.do_interrupt
                        flush_worker_checkpoint!(epmap_eloop, pid)
                        pop!(localresults, pid)
                        put!(epmap_eloop.events, WorkerFreed(pid, action.bad_pid, :map))
                        break
                    end
                end
            end
        catch e
            @warn "uncaught exception in map worker loop" pid
            logerror(e, Logging.Warn; pid)
            @debug "map, putting $pid onto remove channel"
            isopen(epmap_eloop.events) && put!(epmap_eloop.events, WorkerFreed(pid, true, :map))
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
            logerror(e, Logging.Debug)
        end
    end
    @debug "map, deleted the orphaned checkpoints"

    nothing
end

function epmapreduce_reduce!(result::T, epmap_eloop, epmap_journal, options) where {T}
    @debug "entered the reduce method"
    orphans_remove = Set{Any}()

    # Seeding with a large value to account for potential throttling of various cloud storage services
    reduce_times = Float64[300.0]
    rm_times = Float64[300.0]

    @sync while true
        @debug "reduce, interrupted=$(is_interrupted(epmap_eloop))"
        is_interrupted(epmap_eloop) && break
        pid = take!(epmap_eloop.pid_channel_reduce_add)

        @debug "reduce, pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel when the reduction is done

        hostname = ""
        try
            hostname = remotecall_fetch_timeout(60, 1, 1, nothing, tsk->nothing, tsk->nothing, 0, options.gethostname, pid)
        catch e
            @warn "unable to determine host name for pid=$pid"
            logerror(e, Logging.Debug)
            put!(epmap_eloop.events, WorkerFreed(pid, true, :reduce))
            continue
        end

        @async try
            while true
                @debug "reduce, pid=$pid, epmap_eloop.interrupted=$(is_interrupted(epmap_eloop))"
                is_interrupted(epmap_eloop) && break

                set_reduce_dirty!(epmap_eloop, pid, true)

                @debug "reduce, waiting for lock on pid=$pid"
                lock(epmap_eloop.state_lock)
                @debug "reduce, got lock on pid=$pid"

                n_checkpoints = epmap_eloop.is_reduce_triggered ? length(epmap_eloop.reduce_checkpoints_snapshot) : length(epmap_eloop.reduce_checkpoints)

                @debug "reduce, pid=$pid, at exit condition, n_checkpoints=$n_checkpoints"
                if n_checkpoints < 2
                    unlock(epmap_eloop.state_lock)
                    @debug "reduce, popping pid=$pid from dirty list"
                    pop_reduce_dirty!(epmap_eloop, pid)
                    @debug "reduce, putting $pid onto reduce remove channel"
                    put!(epmap_eloop.events, WorkerFreed(pid, false, :reduce))
                    @debug "reduce, done putting $pid onto reduce remove channel"
                    break
                end

                # reduce two checkpoints into a third checkpoint
                local checkpoint1, checkpoint2, checkpoint3
                local pop_error = nothing
                local n_reduce_workers = length(epmap_eloop.used_pids_reduce)
                local is_snapshot = epmap_eloop.is_reduce_triggered

                try
                    if epmap_eloop.is_reduce_triggered
                        checkpoint1 = popfirst!(epmap_eloop.reduce_checkpoints_snapshot)
                        icheckpoint1 = findfirst(checkpoint->checkpoint == checkpoint1, epmap_eloop.reduce_checkpoints)
                        icheckpoint1 === nothing || deleteat!(epmap_eloop.reduce_checkpoints, icheckpoint1)
                    else
                        checkpoint1 = popfirst!(epmap_eloop.reduce_checkpoints)
                    end
                catch e
                    pop_error = e
                    epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                    unlock(epmap_eloop.state_lock)
                    @warn "reduce, unable to pop checkpoint 1"
                    logerror(e, Logging.Debug)
                    continue
                end

                try
                    if epmap_eloop.is_reduce_triggered
                        checkpoint2 = popfirst!(epmap_eloop.reduce_checkpoints_snapshot)
                        icheckpoint2 = findfirst(checkpoint->checkpoint == checkpoint2, epmap_eloop.reduce_checkpoints)
                        icheckpoint2 === nothing || deleteat!(epmap_eloop.reduce_checkpoints, icheckpoint2)
                    else
                        checkpoint2 = popfirst!(epmap_eloop.reduce_checkpoints)
                    end
                catch e
                    push!(epmap_eloop.reduce_checkpoints, checkpoint1)
                    epmap_eloop.is_reduce_triggered && push!(epmap_eloop.reduce_checkpoints_snapshot, checkpoint1)
                    epmap_eloop.reduce_checkpoints_is_dirty[pid] = false
                    unlock(epmap_eloop.state_lock)
                    @warn "reduce, unable to pop checkpoint 2"
                    logerror(e, Logging.Debug)
                    continue
                end

                checkpoint3 = next_checkpoint(options.id, options.scratch)
                unlock(epmap_eloop.state_lock)
                @debug "reduce, released lock on pid=$pid"

                @info "reducing from $n_checkpoints $(is_snapshot ? "snapshot " : "")checkpoints using process $pid ($(nworkers()) julia workers, $(options.nworkers()) provisioned workers, $n_reduce_workers reduce workers)."

                try
                    @debug "reducing into checkpoint3, pid=$pid" checkpoint3
                    journal_start!(epmap_journal; stage="reduce", tsk=0, pid, hostname)
                    # We don't have a good way for estimating the number of reduction tasks (due to the dynamic nature of the resources), so we choose an arbibrary number (10).
                    remotecall_wait_timeout(reduce_times, 10, options.timeout_multiplier, nothing, tsk->nothing, tsk->nothing, 0, reduce, pid, options.reducer!, options.save_checkpoint, options.epmapreduce_fetch, options.load_checkpoint, checkpoint1, checkpoint2, checkpoint3, T)
                    journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=false)
                    push_reduce_checkpoint_and_snapshot!(epmap_eloop, checkpoint3)
                    @debug "pushed reduced checkpoint3, pid=$pid" checkpoint3
                catch e
                    lock(epmap_eloop.state_lock) do
                        push!(epmap_eloop.reduce_checkpoints, checkpoint1, checkpoint2)
                        epmap_eloop.is_reduce_triggered && push!(epmap_eloop.reduce_checkpoints_snapshot, checkpoint1, checkpoint2)
                    end
                    @warn "reduce operation failed" pid hostname
                    action = handle_exception(e, pid, hostname, epmap_eloop, options.maxerrors, options.retries)
                    journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                    action.do_interrupt && set_interrupted!(epmap_eloop)
                    action.do_error && set_errored!(epmap_eloop)
                    if action.do_break || action.do_interrupt
                        pop_reduce_dirty!(epmap_eloop, pid)
                        put!(epmap_eloop.events, WorkerFreed(pid, action.bad_pid, :reduce))
                        set_reduce_dirty!(epmap_eloop, pid, false)
                        break
                    end
                    set_reduce_dirty!(epmap_eloop, pid, false)
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
                    @warn "reduce checkpoint 1 remove failed" pid hostname checkpoint=checkpoint1
                    action = handle_exception(e, pid, hostname, epmap_eloop, options.maxerrors, options.retries)
                    journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                    push!(orphans_remove, checkpoint1, checkpoint2)
                    action.do_interrupt && set_interrupted!(epmap_eloop)
                    action.do_error && set_errored!(epmap_eloop)
                    if action.do_break || action.do_interrupt
                        pop_reduce_dirty!(epmap_eloop, pid)
                        put!(epmap_eloop.events, WorkerFreed(pid, action.bad_pid, :reduce))
                        break
                    end
                    set_reduce_dirty!(epmap_eloop, pid, false)
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
                    @warn "reduce checkpoint 2 remove failed" pid hostname checkpoint=checkpoint2
                    action = handle_exception(e, pid, hostname, epmap_eloop, options.maxerrors, options.retries)
                    journal_stop!(epmap_journal; stage="reduce", tsk=0, pid, fault=true)
                    push!(orphans_remove, checkpoint2)
                    action.do_interrupt && set_interrupted!(epmap_eloop)
                    action.do_error && set_errored!(epmap_eloop)
                    if action.do_break || action.do_interrupt
                        pop_reduce_dirty!(epmap_eloop, pid)
                        put!(epmap_eloop.events, WorkerFreed(pid, action.bad_pid, :reduce))
                        break
                    end
                end

                set_reduce_dirty!(epmap_eloop, pid, false)
            end
        catch e
            @warn "uncaught exception in reduce worker loop" pid
            logerror(e, Logging.Warn; pid)
            @debug "reduce, putting $pid onto remove channel"
            isopen(epmap_eloop.events) && put!(epmap_eloop.events, WorkerFreed(pid, true, :reduce))
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
            @warn "failed to reduce from checkpoint on master" retry=i retries=10 sleep=s
            logerror(e, Logging.Warn)
            if i == 10 || !isfile(epmap_eloop.reduce_checkpoints[1])
                throw(e)
            end
            sleep(s)
        end
    end
    @debug "reduce, finished final reduction on master"

    # Save partial reduction BEFORE deleting checkpoints — the file must still exist.
    if epmap_eloop.is_reduce_triggered
        save_partial_reduction(epmap_eloop)
    end

    @debug "deleting orphaned checkpoints"
    if !(options.keepcheckpoints)
        for checkpoint in epmap_eloop.reduce_checkpoints
            try
                options.rm_checkpoint(checkpoint)
            catch e
                @warn "unable to remove final checkpoint" checkpoint
                logerror(e, Logging.Debug)
            end
        end
        for checkpoint in orphans_remove
            try
                options.rm_checkpoint(checkpoint)
            catch e
                @warn "unable to remove orphan checkpoint" checkpoint
                logerror(e, Logging.Debug)
            end
        end
    end
    @debug "reduce, finished final clean-up on master"

    result
end
