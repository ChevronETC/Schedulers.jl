function default_threadpool_checkpoint_call(preempt_channel_future, checkpoint_task, restart_task, tsk, f, args...; kwargs...)
    # see: https://github.com/JuliaLang/julia/issues/53217
    # We explicitly use a default thread here.  If we don't do this, then the work is sent to the
    # interactive thread pool, and we do not want to block an interactive thread.  For example,
    # in AzManagers.jl we use the interactive thread pool to poll for spot evictions.
    t = Threads.@spawn begin
        try
            restart_task(tsk)
        catch e
            @warn "error restarting task $tsk"
            logerror(e, Logging.Debug)
        end
        f(args...; kwargs...)
    end

    if preempt_channel_future !== nothing
        preempt_channel = fetch(preempt_channel_future)::Channel{Bool}
        preempted = Threads.Atomic{Bool}(false)

        t_preempt = @async begin
            take!(preempt_channel)  # blocks until preemption signal — no polling
            Threads.atomic_xchg!(preempted, true)
            # Interrupt f() first — do NOT checkpoint while f() is still running
            istaskdone(t) || @async Base.throwto(t, InterruptException())
        end

        try
            fetch(t)
        catch e
            # Clean up preempt watcher if work failed for non-preempt reason
            istaskdone(t_preempt) || @async Base.throwto(t_preempt, InterruptException())
            if !preempted[]
                rethrow()
            end
            # If preempted, fall through to checkpoint below
        end

        # Normal completion — clean up blocked preempt watcher
        if !istaskdone(t_preempt)
            @async Base.throwto(t_preempt, InterruptException())
        end

        # f() is guaranteed done here (completed or interrupted). Safe to checkpoint.
        if preempted[]
            try
                checkpoint_task(tsk)
            catch e
                @warn "error checkpointing task $tsk"
                logerror(e, Logging.Debug)
            end
            throw(PreemptException())
        end
    else
        fetch(t)
    end
end
