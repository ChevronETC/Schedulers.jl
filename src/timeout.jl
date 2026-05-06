maximum_task_time(tsk_times, tsk_count, timeout_multiplier) = length(tsk_times) > max(0, floor(Int, 0.5*tsk_count)) ? maximum(tsk_times)*timeout_multiplier : Inf

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
        if time() - tic > maximum_task_time(tsk_times, tsk_count, timeout_multiplier)
            throw(TimeoutException(pid, time() - tic))
        end
        sleep(1)
    end
    isa(tsk_times, AbstractArray) && push!(tsk_times, time() - tic)
    fetch(t)
end

function robust_average(tsk_times, null_tsk_runtime_threshold, tsk_count, tsk, report)
    my_extrema(x) = isempty(x) ? (0.0, 0.0) : extrema(x)

    tsk_times_robust = filter(tsk_time->tsk_time > null_tsk_runtime_threshold, tsk_times)
    if report
        @debug "calculating robust average for tsk=$tsk, length(tsk_times)=$(length(tsk_times)), length(tsk_times_robust)=$(length(tsk_times_robust)), tsk_count=$tsk_count, null_tsk_runtime_threshold=$null_tsk_runtime_threshold, tsk_times=$(my_extrema(tsk_times)), tsk_times_robust=$(my_extrema(tsk_times_robust))"
    end
    if length(tsk_times_robust) >= 0.3 * tsk_count     # Start statistics after 30% of the tasks are done
        return sum(tsk_times_robust) / length(tsk_times_robust)
    else
        return Inf
    end
end

function check_timeout_status(tic, tsk_times, tsk_count, timeout_function_multiplier, grace_period_start_time, null_tsk_runtime_threshold, grace_period_ratio, tsk, report)
    toc = time()
    average_task_time = robust_average(tsk_times, null_tsk_runtime_threshold, tsk_count, tsk, report)
    is_timeout_function,is_timeout_grace = false,false
    if toc - tic > average_task_time * timeout_function_multiplier
        is_timeout_function = true
    elseif toc - grace_period_start_time > grace_period_ratio * average_task_time
        is_timeout_grace = true
    end
    report && @debug "checking timeout status, tsk=$tsk, toc-tic=$(toc - tic), average_task_time=$average_task_time, timeout_function_multiplier=$timeout_function_multiplier, grace_period_start_time=$(isinf(grace_period_start_time) ? "Inf" : unix2datetime(grace_period_start_time)), grace_period_ratio=$grace_period_ratio, is_timeout_function=$is_timeout_function, is_timeout_grace=$is_timeout_grace"
    return is_timeout_function || is_timeout_grace
end

function remotecall_func_wait_timeout(tsk_times, eloop, options, preempt_channel_future, checkpoint_task, restart_task, tsk, f, pid, args...; kwargs...)
    t = @async remotecall_wait(default_threadpool_checkpoint_call, pid, preempt_channel_future, checkpoint_task, restart_task, tsk, f, args...; kwargs...)
    tic = time()
    tic_report = time()
    while !istaskdone(t)
        report = false
        if time() - tic_report > 600
            report = true
            tic_report = time()
        end
        if check_timeout_status(tic, tsk_times, eloop.tsk_count, options.timeout_function_multiplier, eloop.grace_period_start_time, options.null_tsk_runtime_threshold, options.grace_period_ratio, tsk, report)
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
