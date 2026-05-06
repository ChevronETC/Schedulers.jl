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
* `null_tsk_runtime_threshold=0` the maximum duration (in seconds) for a task to be considered insignificant or 'null' and thus not included in the timeout measurement.
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
* `journal_init_callback=tsks->nothing` additional method when initializing the journal
* `journal_task_callback=tsk->nothing` additional method when journaling a task
# GXTODO: add doc
## Notes
[1] The number of machines provisioned may be greater than the number of workers in the cluster since with
some cluster managers, there may be a delay between the provisioining of a machine, and when it is added to the
Julia cluster.
[2] For example, on Azure Cloud a SPOT instance will be pre-emptied if someone is willing to pay more for it
"""
function epmap(options::SchedulerOptions, f::Function, tasks, args...; kwargs...)
    eloop = ElasticLoop(Nothing, tasks, options; isreduce=false)
    journal = journal_init(tasks, options.journal_init_callback; reduce=false)

    tsk_map = @async epmap_map(options, f, tasks, eloop, journal, args...; kwargs...)
    loop(eloop, journal, options, tsk_map, @async nothing)
    fetch(tsk_map)
end

function epmap_map(options::SchedulerOptions, f::Function, tasks, eloop::ElasticLoop, journal, args...; kwargs...)
    tsk_times = Float64[]

    # work loop
    @sync while true
        is_interrupted(eloop) && break
        pid = take!(eloop.pid_channel_map_add)

        @debug "pid=$pid"
        pid == -1 && break # pid=-1 is put onto the channel in the above elastic_loop when tsk_pool_done is full.

        hostname = ""

        local preempt_channel_future
        try
            preempt_channel_future = options.preempt_channel_future(pid)
        catch e
            @warn "failed to retrieve preempt_channel_future, checkpoint/restart functionality disabled" pid
            logerror(e, Logging.Debug)
            preempt_channel_future = nothing
        end

        @async try
            while true
                if hostname == ""
                    try
                        hostname = remotecall_fetch_timeout(60, 1, 1, nothing, tsk->nothing, tsk->nothing, 0, options.gethostname, pid)
                        set_task_context!(; worker_pid=pid, hostname=hostname)
                    catch e
                        @warn "unable to determine hostname for pid=$pid within 60 seconds"
                        logerror(e, Logging.Debug)
                        put!(eloop.events, WorkerFreed(pid, true, :map))
                        break
                    end
                end

                @debug "map task loop exit condition" pid tasks_remaining(eloop) is_interrupted(eloop)
                if tasks_remaining(eloop) == 0 || is_interrupted(eloop)
                    @debug "putting $pid onto map_remove channel"
                    put!(eloop.events, WorkerFreed(pid, false, :map))
                    break
                end

                tsk = pop_next_task!(eloop)
                tsk === nothing && (yield(); continue)

                set_task_context!(; task_id=tsk)
                try
                    options.reporttasks && @info "running task $tsk on process $pid ($hostname); $(nworkers()) julia workers total; $(options.nworkers()) provisioned workers total; $(tasks_remaining(eloop)) tasks left in task-pool."
                    yield()
                    journal_start!(journal, options.journal_task_callback; stage="tasks", tsk, pid, hostname)
                    remotecall_func_wait_timeout(tsk_times, eloop, options, preempt_channel_future, options.checkpoint_task, options.restart_task, tsk, f, pid, tsk, args...; kwargs...)
                    journal_stop!(journal, options.journal_task_callback; stage="tasks", tsk, pid, fault=false)
                    mark_task_done!(eloop, tsk)
                    @debug "...pid=$pid,tsk=$tsk,nworkers()=$(nworkers()),options.nworkers()=$(options.nworkers()), tasks_remaining=$(tasks_remaining(eloop)), tasks_done=$(tasks_done_count(eloop)) -!"
                    yield()
                catch e
                    @warn "task execution failed" pid hostname tsk failures=get_pid_failures(eloop, pid)
                    journal_stop!(journal, options.journal_task_callback; stage="tasks", tsk, pid, fault=true)
                    action = handle_exception(e, pid, hostname, eloop, options.maxerrors, options.retries)
                    actual_e = e isa TaskFailedException ? e.task.result : e
                    if isa(actual_e, TimeoutException) && options.skip_tasks_that_timeout
                        @warn "skipping task that timed out" tsk pid
                        mark_task_timed_out!(eloop, tsk)
                    elseif !action.retry_task
                        if !tsk_was_retried(eloop, tsk)
                            @warn "task failed, allowing one cross-worker retry" tsk pid
                            tsk_retried_add!(eloop, tsk)
                            requeue_task!(eloop, tsk)
                        else
                            @warn "task permanently failed after cross-worker retry" tsk pid
                            mark_task_timed_out!(eloop, tsk)
                        end
                    else
                        requeue_task!(eloop, tsk)
                    end
                    apply_exception_action!(eloop, action, pid)
                    action.do_break && break
                end
            end
        catch e
            @warn "uncaught exception in worker loop for pid=$pid"
            logerror(e, Logging.Debug)
            @debug "putting $pid onto remove channel"
            isopen(eloop.events) && put!(eloop.events, WorkerFreed(pid, true, :map))
            @debug "done putting $pid onto remove channel"
        finally
            clear_task_context!()
        end
    end
    @debug "exiting the map loop"

    journal_final(journal)

    journal, eloop.tsk_pool_timed_out
end

epmap(f::Function, tasks, args...; kwargs...) = epmap(SchedulerOptions(), f, tasks, args..., kwargs...)
