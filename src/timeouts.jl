"""
Timeout handling functions for task execution.
"""

maximum_task_time(tsk_times, tsk_count, timeout_multiplier) =
    length(tsk_times) > max(0, floor(Int, 0.5*tsk_count)) ?
    maximum(tsk_times)*timeout_multiplier : Inf

function default_threadpool_checkpoint_call(
    preempt_channel_future,
    checkpoint_task,
    restart_task,
    tsk,
    f,
    args...;
    kwargs...,
)
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

function remotecall_wait_timeout(
    tsk_times,
    tsk_count,
    timeout_multiplier,
    preempt_channel_future,
    checkpoint_task,
    restart_task,
    tsk,
    f,
    pid,
    args...;
    kwargs...,
)
    t = @async remotecall_wait(
        default_threadpool_checkpoint_call,
        pid,
        preempt_channel_future,
        checkpoint_task,
        restart_task,
        tsk,
        f,
        args...;
        kwargs...,
    )
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

function remotecall_fetch_timeout(
    tsk_times,
    tsk_count,
    timeout_multiplier,
    preempt_channel_future,
    checkpoint_task,
    restart_task,
    tsk,
    f,
    pid,
    args...;
    kwargs...,
)
    t = @async remotecall_fetch(
        default_threadpool_checkpoint_call,
        pid,
        preempt_channel_future,
        checkpoint_task,
        restart_task,
        tsk,
        f,
        args...;
        kwargs...,
    )
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

function remotecall_func_wait_timeout(
    tsk_times,
    eloop,
    options,
    preempt_channel_future,
    checkpoint_task,
    restart_task,
    tsk,
    f,
    pid,
    args...;
    kwargs...,
)
    t = @async remotecall_wait(
        default_threadpool_checkpoint_call,
        pid,
        preempt_channel_future,
        checkpoint_task,
        restart_task,
        tsk,
        f,
        args...;
        kwargs...,
    )
    tic = time()
    while !istaskdone(t)
        if check_timeout_status(
            tic,
            tsk_times,
            eloop.tsk_count,
            options.timeout_function_multiplier,
            eloop.grace_period_start_time,
            options.null_tsk_runtime_threshold,
            options.grace_period_ratio,
        )
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

remotecall_default_threadpool(f, pid, args...; kwargs...) = remotecall(
    default_threadpool_checkpoint_call,
    pid,
    nothing,
    tsk->nothing,
    tsk->nothing,
    0,
    f,
    args...;
    kwargs...,
)
