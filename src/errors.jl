function handle_exception(e::PreemptException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    @warn "preempt exception caught on process with id=$pid ($hostname)"
    (bad_pid = true, do_break=true, do_interrupt=false, do_error=true)
end

function handle_exception(e::TimeoutException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    logerror(e, Logging.Debug)

    fails[pid] += 1
    nerrors = sum(values(fails))

    # If the task times out than we make the conservative assumption that there might be something
    # wrong with the machine it is running on, hence we set bad_pid=true.
    r = (bad_pid = true, do_break=true, do_interrupt=false, do_error=false)

    if nerrors >= epmap_maxerrors
        @error "too many total errors, $nerrors errors"
        r = (bad_pid = true, do_break=true, do_interrupt=true, do_error=true)
    end

    r
end

# the use of async tasks to allow for timeout logic means we need to handle TaskFailedException's which, in turn, wraps their tasks whose result should be an exception:
function handle_exception(e::TaskFailedException, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    handle_exception(e.task.result, pid, hostname, fails, epmap_maxerrors, epmap_retries)
end

function handle_exception(e, pid, hostname, fails, epmap_maxerrors, epmap_retries)
    logerror(e, Logging.Debug)

    fails[pid] += 1
    nerrors = sum(values(fails))

    r = (bad_pid = false, do_break=false, do_interrupt=false, do_error=false)

    if isa(e, InterruptException)
        r = (bad_pid = false, do_break=false, do_interrupt=true, do_error=false)
    elseif isa(e, ProcessExitedException)
        @warn "process with id=$pid ($hostname) exited, marking as bad for removal."
        r = (bad_pid = true, do_break=true, do_interrupt=false, do_error=false)
    elseif fails[pid] > epmap_retries
        @warn "too many failures on process with id=$pid ($hostname), removing from process list"
        r = (bad_pid = true, do_break=true, do_interrupt=false, do_error=false)
    end

    if nerrors >= epmap_maxerrors
        @error "too many total errors, $nerrors errors"
        r = (bad_pid = false, do_break=true, do_interrupt=true, do_error=true)
    end

    r
end
