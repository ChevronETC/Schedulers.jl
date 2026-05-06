function load_modules_on_new_workers(pid)
    _names = names(Main; imported=true)
    for _name in _names
        try
            if isa(Base.eval(Main, _name), Module) && _name ∉ (:Base, :Core, :InteractiveUtils, :VSCodeServer, :Main, :_vscodeserver)
                remotecall_fetch(Base.eval, pid, Main, :(using $_name))
            end
        catch e
            @debug "caught error in load_modules_on_new_workers for module $_name"
            logerror(e, Logging.Debug)
        end
    end
    nothing
end

function load_functions_on_new_workers(pid)
    ignore = (Symbol("@enter"), Symbol("@run"), :ans, :eval, :include, :vscodedisplay)
    _names = filter(name->name ∉ ignore && isa(Base.eval(Main, name), Function), names(Main; imported=true))

    for _name in _names
        try
            remotecall_fetch(Base.eval, pid, Main, :(function $_name end))
        catch e
            @debug "caught error in load_functions_on_new_workers (function) for pid '$pid' and function '$_name'"
            logerror(e, Logging.Debug)
        end
    end

    for _name in _names
        for method in Base.eval(Main, :(methods($_name)))
            try
                remotecall_fetch(Base.eval, pid, Main, :($method))
            catch e
                @debug "caught error in load_functions_on_new_workers (methods) for pid '$pid', function '$_name', method '$method'"
                logerror(e, Logging.Debug)
            end
        end
    end
    nothing
end

function robust_rmprocs(pids; waitfor)
    try
        rmprocs(pids; waitfor)
    catch e
        @warn "unable to run rmprocs on $pids, using fall-back strategy"
        logerror(e, Logging.Debug)
        try
            rmprocset = Union{Distributed.LocalProcess, Distributed.Worker}[]
            for pid in pids
                if pid != 1 && haskey(Distributed.map_pid_wrkr, pid)
                    w = Distributed.map_pid_wrkr[pid]
                    push!(rmprocset, w)
                end
            end
            unremoved = [wrkr.id for wrkr in filter(w -> w.state !== Distributed.W_TERMINATED, rmprocset)]

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
            logerror(e, Logging.Debug)
        end
    end
end
