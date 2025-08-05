
"""
    results, timed_out_tasks = epmap_collect(options, f, tasks, args...; kwargs...)

Similar to `epmap` but collects and returns the results from each task execution.
The function `f` should return a value that will be collected.

Returns a tuple of:
- `results`: Vector of results in the same order as input tasks
- `timed_out_tasks`: Vector of tasks that timed out

Uses the same parameters as `epmap` but with result collection enabled.
"""
function epmap_collect(options::SchedulerOptions, f::Function, tasks, args...; kwargs...)
    eloop = ElasticLoop(Nothing, tasks, options; isreduce = false)
    journal = journal_init(tasks, options.journal_init_callback; reduce = false)

    results = Dict{Any,Any}()  # Map task -> result

    tsk_map = @async epmap_map_collect(
        options,
        f,
        tasks,
        eloop,
        journal,
        results,
        args...;
        kwargs...,
    )
    loop(eloop, journal, options.journal_task_callback, tsk_map, @async nothing)
    fetch(tsk_map)

    # Return results in task order, with nothing for failed/timed-out tasks
    ordered_results = [get(results, task, nothing) for task in tasks]
    ordered_results, eloop.tsk_pool_timed_out
end

function epmap_map_collect(
    options::SchedulerOptions,
    f::Function,
    tasks,
    eloop::ElasticLoop,
    journal,
    results::Dict,
    args...;
    kwargs...,
)
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
            @warn "failed to retrieve preempt_channel_future.  checkpoint/restart functionality disabled."
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
                        @warn "unable to determine hostname for pid=$pid within 60 seconds"
                        logerror(e, Logging.Warn)
                        put!(eloop.pid_channel_map_remove, (pid, true))
                        break
                    end
                end

                @debug "map task loop exit condition" pid length(eloop.tsk_pool_todo) eloop.interrupted
                if length(eloop.tsk_pool_todo) == 0 || eloop.interrupted
                    @debug "putting $pid onto map_remove channel"
                    put!(eloop.pid_channel_map_remove, (pid, false))
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
                    options.reporttasks &&
                        @info "running task $tsk on process $pid ($hostname); $(nworkers()) workers total; $(length(eloop.tsk_pool_todo)) tasks left in task-pool."
                    yield()
                    journal_start!(
                        journal,
                        options.journal_task_callback;
                        stage = "tasks",
                        tsk,
                        pid,
                        hostname,
                    )

                    # Use remotecall_fetch_timeout to get the result
                    result = remotecall_fetch_timeout(
                        tsk_times,
                        eloop.tsk_count,
                        options.timeout_function_multiplier,
                        preempt_channel_future,
                        options.checkpoint_task,
                        options.restart_task,
                        tsk,
                        f,
                        pid,
                        tsk,
                        args...;
                        kwargs...,
                    )

                    journal_stop!(
                        journal,
                        options.journal_task_callback;
                        stage = "tasks",
                        tsk,
                        pid,
                        fault = false,
                    )

                    # Store the result
                    results[tsk] = result
                    push!(eloop.tsk_pool_done, tsk)
                    @debug "...pid=$pid,tsk=$tsk,nworkers()=$(nworkers()), tsk_pool_todo=$(eloop.tsk_pool_todo), tsk_pool_done=$(eloop.tsk_pool_done) -!"
                    yield()
                catch e
                    @warn "caught an exception, there have been $(eloop.pid_failures[pid]) failure(s) on process $pid ($hostname)..."
                    journal_stop!(
                        journal,
                        options.journal_task_callback;
                        stage = "tasks",
                        tsk,
                        pid,
                        fault = true,
                    )
                    if isa(e, TimeoutException) && options.skip_tasks_that_timeout
                        @warn "skipping task '$tsk' that timed out"
                        push!(eloop.tsk_pool_done, tsk)
                        push!(eloop.tsk_pool_timed_out, tsk)
                        # Don't store result for timed out tasks
                    else
                        push!(eloop.tsk_pool_todo, tsk)
                    end
                    r = handle_exception(
                        e,
                        pid,
                        hostname,
                        eloop.pid_failures,
                        options.maxerrors,
                        options.retries,
                    )
                    if r.do_break || r.do_interrupt
                        put!(eloop.pid_channel_map_remove, (pid, r.bad_pid))
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
            isopen(eloop.pid_channel_map_remove) &&
                put!(eloop.pid_channel_map_remove, (pid, true))
            @debug "done putting $pid onto remove channel"
        end
    end
    @debug "exiting the map loop"

    journal_final(journal)

    journal, eloop.tsk_pool_timed_out
end

epmap_collect(f::Function, tasks, args...; kwargs...) =
    epmap_collect(SchedulerOptions(), f, tasks, args...; kwargs...)
