"""
Utility functions for the Schedulers module.
"""

function logerror(e, loglevel = Logging.Debug)
    io = IOBuffer()
    showerror(io, e)
    write(io, "\n\terror type: $(typeof(e))\n")
    if VERSION >= v"1.7"
        show(io, current_exceptions())
    else
        for (exc, bt) in Base.catch_stack()
            showerror(io, exc, bt)
            println(io)
        end
    end
    @logmsg loglevel String(take!(io))
end

# see https://github.com/JuliaTime/TimeZones.jl/issues/374
now_formatted() = Dates.format(now(Dates.UTC), dateformat"yyyy-mm-dd\THH:MM:SS\Z")

function robust_average(tsk_times, null_tsk_runtime_threshold, tsk_count)
    tsk_times_robust = filter(tsk_time->tsk_time > null_tsk_runtime_threshold, tsk_times)
    if length(tsk_times_robust) >= 0.3 * tsk_count     # Start statistics after 30% of the tasks are done
        return sum(tsk_times_robust) / length(tsk_times_robust)
    else
        return Inf
    end
end

function check_timeout_status(
    tic,
    tsk_times,
    tsk_count,
    timeout_function_multiplier,
    grace_period_start_time,
    null_tsk_runtime_threshold,
    grace_period_ratio,
)
    toc = time()
    average_task_time = robust_average(tsk_times, null_tsk_runtime_threshold, tsk_count)
    if toc - tic > average_task_time * timeout_function_multiplier
        is_timeout = true
    elseif toc - grace_period_start_time > grace_period_ratio * average_task_time
        is_timeout = true
    else
        is_timeout = false
    end
    return is_timeout
end

function robust_rmprocs(pids; waitfor)
    try
        rmprocs(pids; waitfor)
    catch e
        @warn "unable to run rmprocs on $pids, using fall-back strategy"
        logerror(e, Logging.Warn)
        try
            rmprocset = Union{Distributed.LocalProcess,Distributed.Worker}[]
            for pid in pids
                if pid != 1 && haskey(Distributed.map_pid_wrkr, pid)
                    w = Distributed.map_pid_wrkr[pid]
                    push!(rmprocset, w)
                end
            end
            unremoved = [
                wrkr.id for
                wrkr in filter(w -> w.state !== Distributed.W_TERMINATED, rmprocset)
            ]

            lock(Distributed.worker_lock)
            try
                for pid in unremoved
                    @debug "robust_rmprocs, setting worker state, and calling kill"
                    if haskey(Distributed.map_pid_wrkr, pid)
                        w = Distributed.map_pid_wrkr[pid]
                        Distributed.set_worker_state(w, Distributed.W_TERMINATED)
                        Distributed.deregister_worker(pid)
                        Distributed.kill(w.manager, pid, w.config)
                    end
                end
            catch
            finally
                unlock(Distributed.worker_lock)
            end
        catch e
            @warn "rmprocs fall-back strategy failed."
            logerror(e, Logging.Error)
        end
    end
end
