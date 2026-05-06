function logerror(e, loglevel=Logging.Debug; kwargs...)
    io = IOBuffer()
    showerror(io, e)
    write(io, "\n\terror type: $(typeof(e))\n")
    if !isempty(kwargs)
        write(io, "\tcontext:")
        for (k, v) in pairs(kwargs)
            write(io, " $k=$v")
        end
        write(io, "\n")
    end
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
