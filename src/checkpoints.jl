"""
Checkpoint management for fault tolerance and partial reduction.
"""

let CID::Int = 1
    global next_checkpoint_id
    next_checkpoint_id() = (id = CID; CID += 1; id)
end

let SID::Int = 1
    global next_scratch_index
    next_scratch_index(n) = (id = clamp(SID, 1, n); SID = id == n ? 1 : id+1; id)
end

function next_checkpoint(id, scratch)
    joinpath(
        scratch[next_scratch_index(length(scratch))],
        string("checkpoint-", id, "-", next_checkpoint_id()),
    )
end

function reduce(
    reducer!,
    save_checkpoint_method,
    fetch_method,
    load_checkpoint_method,
    checkpoint1,
    checkpoint2,
    checkpoint3,
    ::Type{T},
) where {T}
    @debug "reduce, load checkpoint 1"
    c1 = load_checkpoint(load_checkpoint_method, checkpoint1, T)
    @debug "reduce, load checkpoint 2"
    c2 = load_checkpoint(load_checkpoint_method, checkpoint2, T)
    @debug "reduce, reducer"
    reducer!(c2, c1)
    @debug "reduce, serialize"
    save_checkpoint(save_checkpoint_method, fetch_method, checkpoint3, c2, T)
    @debug "reduce, done"
    nothing
end

save_checkpoint(
    save_checkpoint_method,
    fetch_method,
    checkpoint,
    _localresult,
    ::Type{T},
) where {T} = (save_checkpoint_method(checkpoint, fetch_method(_localresult)::T); nothing)
load_checkpoint(load_checkpoint_method, checkpoint, ::Type{T}) where {T} =
    load_checkpoint_method(checkpoint)::T
