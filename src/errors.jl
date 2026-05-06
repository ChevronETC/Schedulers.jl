function handle_exception(e::PreemptException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    @warn "preempt exception caught on process with id=$pid ($hostname)"
    ExceptionAction(true, true, false, true)
end

function handle_exception(e::TimeoutException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    logerror(e, Logging.Warn; pid, hostname)

    fails[pid] += 1
    nerrors = sum(values(fails))

    # If the task times out then we make the conservative assumption that there might be something
    # wrong with the machine it is running on, hence we set bad_pid=true.
    if nerrors >= epmap_maxerrors
        @error "too many total errors" nerrors maxerrors=epmap_maxerrors pid hostname
        ExceptionAction(true, true, true, true)
    else
        ExceptionAction(true, true, false, false)
    end
end

function handle_exception(e::InterruptException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    ExceptionAction(false, false, true, false)
end

function handle_exception(e::ProcessExitedException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    @warn "process with id=$pid ($hostname) exited, marking as bad for removal."
    fails[pid] += 1
    nerrors = sum(values(fails))
    if nerrors >= epmap_maxerrors
        @error "too many total errors" nerrors maxerrors=epmap_maxerrors pid hostname
        ExceptionAction(true, true, true, true)
    else
        ExceptionAction(true, true, false, false)
    end
end

# TaskFailedException wraps the actual exception in the task's result:
function handle_exception(e::TaskFailedException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    handle_exception(e.task.result, pid, hostname, fails, epmap_maxerrors, epmap_retries)
end

function handle_exception(e, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    logerror(e, Logging.Warn; pid, hostname)

    fails[pid] += 1
    nerrors = sum(values(fails))

    if fails[pid] > epmap_retries
        @warn "too many failures on process with id=$pid ($hostname), removing from process list" failures=fails[pid] retries=epmap_retries
        if nerrors >= epmap_maxerrors
            @error "too many total errors" nerrors maxerrors=epmap_maxerrors pid hostname
            ExceptionAction(true, true, true, true)
        else
            ExceptionAction(true, true, false, false)
        end
    elseif nerrors >= epmap_maxerrors
        @error "too many total errors" nerrors maxerrors=epmap_maxerrors pid hostname
        ExceptionAction(false, true, true, true)
    else
        ExceptionAction(false, false, false, false)
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
