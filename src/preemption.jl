function default_threadpool_checkpoint_call(preempt_channel_future, checkpoint_task, restart_task, tsk, f, args...; kwargs...)
    # see: https://github.com/JuliaLang/julia/issues/53217
    # We explicitly use a default thread here.  If we don't do this, then the work is sent to the
    # interactive thread pool, and we do not want to block an interactive thread.  For example,
    # in AzManagers.jl we use the interactive thread pool to poll for spot evictions.
    t = Threads.@spawn begin
        try
            restart_task(tsk)
        catch
            @warn "error restarting task $tsk"
            logerror(e, Logging.Debug)
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
                    logerror(e, Logging.Debug)
                end
                @async Base.throwto(t, InterruptException())
                throw(PreemptException())
            end
            sleep(0.1)
        end
    end

    fetch(t)
end
