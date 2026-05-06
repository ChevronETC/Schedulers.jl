"""
    unwrap_exception(e) -> exception

Unwrap wrapper exceptions (TaskFailedException, RemoteException, CapturedException)
to find the root cause exception.
"""
function unwrap_exception(e)
    if e isa TaskFailedException
        return unwrap_exception(e.task.result)
    elseif e isa RemoteException
        return unwrap_exception(e.captured.ex)
    elseif e isa CapturedException
        return unwrap_exception(e.ex)
    else
        return e
    end
end

"""
    root_cause_bt(e) -> Union{Vector, Nothing}

Extract the backtrace closest to the root cause from a wrapped exception chain.
"""
function root_cause_bt(e)
    if e isa TaskFailedException
        inner = e.task.result
        if inner isa RemoteException && inner.captured isa CapturedException
            return inner.captured.processed_bt
        elseif inner isa CapturedException
            return inner.processed_bt
        else
            return root_cause_bt(inner)
        end
    elseif e isa RemoteException && e.captured isa CapturedException
        return e.captured.processed_bt
    elseif e isa CapturedException
        return e.processed_bt
    else
        return nothing
    end
end

function logerror(e, loglevel=Logging.Debug; kwargs...)
    root = unwrap_exception(e)

    if loglevel >= Logging.Warn
        # Concise summary: root cause message + type + context + short backtrace
        io = IOBuffer()
        showerror(io, root)
        write(io, " ($(typeof(root)))")
        if !isempty(kwargs)
            write(io, " —")
            for (k, v) in pairs(kwargs)
                write(io, " $k=$v")
            end
        end

        # Show a short backtrace: only user-code frames (skip Julia internals)
        bt = root_cause_bt(e)
        if bt !== nothing
            user_frames = _user_frames(bt)
            if !isempty(user_frames)
                write(io, "\n  Stacktrace (user code):")
                for frame in user_frames
                    write(io, "\n    $(frame.func) @ $(frame.file):$(frame.line)")
                end
            end
        end

        @logmsg loglevel String(take!(io))
    else
        # Debug: full exception chain for developers
        io = IOBuffer()
        showerror(io, e)
        write(io, "\n\terror type: $(typeof(root))\n")
        if !isempty(kwargs)
            write(io, "\tcontext:")
            for (k, v) in pairs(kwargs)
                write(io, " $k=$v")
            end
            write(io, "\n")
        end
        show(io, current_exceptions())
        @logmsg loglevel String(take!(io))
    end
end

"""
    _user_frames(bt) -> Vector{NamedTuple}

Extract user-code frames from a backtrace, filtering out Julia internals,
Distributed, and Schedulers infrastructure frames.
"""
function _user_frames(bt)
    frames = NamedTuple{(:func, :file, :line), Tuple{String, String, Int}}[]
    try
        for entry in bt
            sf = entry isa Tuple ? entry[1] : entry
            sf isa Base.StackTraces.StackFrame || continue
            file = string(sf.file)
            func = string(sf.func)

            # Skip internal frames by file path and function name
            is_internal = startswith(file, "./") ||
                          contains(file, "julia/stdlib") ||
                          contains(file, "juliaup") ||
                          contains(file, "/julia/") ||
                          endswith(file, "error.jl") ||
                          func == "top-level scope"

            # Also skip if this is a Schedulers internal frame
            if sf.linfo isa Core.MethodInstance
                mod_str = string(sf.linfo.def.module)
                is_internal = is_internal || mod_str == "Schedulers"
            end

            if !is_internal && !startswith(func, "#")
                push!(frames, (func=func, file=basename(file), line=sf.line))
            end
        end
    catch
        # If backtrace parsing fails, return empty — don't break error handling
    end
    frames
end
