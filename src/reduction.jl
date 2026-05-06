function reduce_checkpoints_is_dirty(eloop::ElasticLoop)
    for value in values(eloop.reduce_checkpoints_is_dirty)
        if value
            return value
        end
    end
    false
end

function save_partial_reduction(eloop)
    if isempty(eloop.reduce_checkpoints)
        @warn "reduction is empty, nothing to save."
    else
        try
            x = deserialize(eloop.reduce_checkpoints_snapshot[1])
            empty!(eloop.reduce_checkpoints_snapshot)
            eloop.epmap_save_partial_reduction(x)
        catch e
            @error "problem running user-supplied save_partial_reduction"
            logerror(e, Logging.Debug)
        end
    end
end

trigger_reduction!(eloop::ElasticLoop) = put!(eloop.reduce_trigger_channel, true)

function reduce_trigger(eloop::ElasticLoop, journal, journal_task_callback)
    @debug "running user reduce trigger"
    try
        eloop.epmap_reduce_trigger(eloop)
    catch e
        @error "problem running user-supplied reduce trigger"
        logerror(e, Logging.Debug)
    end

    @debug "checking for reduce trigger"
    if !isempty(eloop.reduce_trigger_channel)
        take!(eloop.reduce_trigger_channel)
        if !(eloop.is_reduce_triggered)
            eloop.is_reduce_triggered = true
            eloop.checkpoints_are_flushed = false
        end
    end

    @debug "eloop.is_reduce_triggered=$(eloop.is_reduce_triggered), length(eloop.checkpoints)=$(length(eloop.checkpoints)), length(eloop.reduce_checkpoints)=$(length(eloop.reduce_checkpoints))"
    if eloop.is_reduce_triggered && eloop.checkpoints_are_flushed && !reduce_checkpoints_is_dirty(eloop) && length(eloop.reduce_checkpoints_snapshot) == 1
        @info "saving partial reduction, length(eloop.checkpoints)=$(length(eloop.checkpoints)), length(eloop.reduce_checkpoints)=$(length(eloop.reduce_checkpoints))"
        save_partial_reduction(eloop)
        @info "done saving partial reduction"
        try
            for tsk in eloop.tsk_pool_done
                if tsk ∉ eloop.tsk_pool_reduced
                    journal_stop!(journal, journal_task_callback; stage="reduced", tsk, pid=0, fault=false)
                end
            end
        catch e
            @warn "error stopping journal"
            logerror(e, Logging.Debug)
        end
        eloop.tsk_pool_reduced = copy(eloop.tsk_pool_done)
        eloop.is_reduce_triggered = false
        eloop.checkpoints_are_flushed = true
    end
    eloop.is_reduce_triggered
end
