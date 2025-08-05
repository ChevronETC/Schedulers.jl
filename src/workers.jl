"""
Worker management functions for distributed computing.
"""

# for performance metrics, track when the pid is started
const _pid_up_timestamp = Dict{Int,Float64}()

function load_modules_on_new_workers(pid)
    _names = names(Main; imported = true)
    for _name in _names
        try
            if isa(Base.eval(Main, _name), Module) &&
               _name ∉
               (:Base, :Core, :InteractiveUtils, :VSCodeServer, :Main, :_vscodeserver)
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
    _names = filter(
        name->name ∉ ignore && isa(Base.eval(Main, name), Function),
        names(Main; imported = true),
    )

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
