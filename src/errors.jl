# --- Worker-fault exceptions: retry the task on a different worker ---

function handle_exception(e::PreemptException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    @warn "preempt exception caught on process with id=$pid ($hostname)"
    ExceptionAction(true, true, false, false, true)
end

function handle_exception(e::TimeoutException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    logerror(e, Logging.Warn; pid, hostname)

    fails[pid] += 1
    nerrors = sum(values(fails))

    # Timeout is attributed to the worker — the task may succeed on a faster machine.
    # The caller checks skip_tasks_that_timeout to decide whether to skip instead of retry.
    if nerrors >= epmap_maxerrors
        @error "too many total errors" nerrors maxerrors=epmap_maxerrors pid hostname
        ExceptionAction(true, true, true, true, true)
    else
        ExceptionAction(true, true, false, false, true)
    end
end

function handle_exception(e::InterruptException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    ExceptionAction(false, false, true, false, false)
end

function handle_exception(e::ProcessExitedException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    @warn "process with id=$pid ($hostname) exited, marking as bad for removal."
    fails[pid] += 1
    nerrors = sum(values(fails))
    if nerrors >= epmap_maxerrors
        @error "too many total errors" nerrors maxerrors=epmap_maxerrors pid hostname
        ExceptionAction(true, true, true, true, true)
    else
        ExceptionAction(true, true, false, false, true)
    end
end

# TaskFailedException wraps the actual exception in the task's result:
function handle_exception(e::TaskFailedException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    handle_exception(e.task.result, pid, hostname, fails, epmap_maxerrors, epmap_retries)
end

# RemoteException wraps the exception from a remote worker:
function handle_exception(e::RemoteException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    handle_exception(e.captured.ex, pid, hostname, fails, epmap_maxerrors, epmap_retries)
end

# CapturedException wraps the exception from a remote worker (via remotecall):
function handle_exception(e::CapturedException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    handle_exception(e.ex, pid, hostname, fails, epmap_maxerrors, epmap_retries)
end

# --- Task-fault exceptions: the user function errored ---
#
# When retries are not exhausted, retry on the SAME worker (the worker is fine).
# When retries are exhausted, the task is permanently failed — don't retry on any worker.
# The worker is NOT marked bad (it can run other tasks fine).

function handle_exception(e, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    logerror(e, Logging.Warn; pid, hostname)

    fails[pid] += 1
    nerrors = sum(values(fails))

    if fails[pid] > epmap_retries
        @warn "task fault: retries exhausted on process with id=$pid ($hostname)" failures=fails[pid] retries=epmap_retries
        if nerrors >= epmap_maxerrors
            @error "too many total errors" nerrors maxerrors=epmap_maxerrors pid hostname
            ExceptionAction(false, true, true, true, false)
        else
            # Worker is not bad — task is at fault. Break to free the worker for other tasks.
            ExceptionAction(false, true, false, false, false)
        end
    elseif nerrors >= epmap_maxerrors
        @error "too many total errors" nerrors maxerrors=epmap_maxerrors pid hostname
        ExceptionAction(false, true, true, true, false)
    else
        # Retry on same worker (retries not exhausted yet)
        ExceptionAction(false, false, false, false, true)
    end
end

"""
    apply_exception_action!(eloop, action, pid; phase=:map) -> action

Apply an `ExceptionAction` to the elastic loop state: set interrupted/errored flags,
and emit a WorkerFreed event if breaking.
Returns the action for further inspection by the caller.
"""
function apply_exception_action!(eloop, action::ExceptionAction, pid; phase::Symbol=:map)
    eloop.interrupted = eloop.interrupted || action.do_interrupt
    eloop.errored = eloop.errored || action.do_error
    if action.do_break || action.do_interrupt
        isopen(eloop.events) && put!(eloop.events, WorkerFreed(pid, action.bad_pid, phase))
    end
    action
end
