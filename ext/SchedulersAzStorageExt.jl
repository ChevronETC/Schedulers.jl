module SchedulersAzStorageExt

using AzStorage, Schedulers

#=
Streaming checkpoints as Azure block blobs: each chunk is uploaded as its own block, and the blob only appears
when its block list is committed.  Blocks that are never committed are discarded by Azure after a week.

AzStorage 2.14 has a public block API.  Earlier versions, still needed next to packages that require HTTP.jl 1,
have the same functionality internally.
=#
if isdefined(AzStorage, :putblock)
    putblock(object, blockid, buffer) = AzStorage.putblock(object, blockid, buffer)
    putblocklist(object, blockids) = AzStorage.putblocklist(object, blockids)
else
    function putblock(object, blockid, buffer)
        GC.@preserve buffer AzStorage.writebytes_block(object.container, object.name, unsafe_wrap(Vector{UInt8}, pointer(buffer), length(buffer)), [blockid])
        nothing
    end
    putblocklist(object, blockids) = AzStorage.putblocklist(object.container, object.name, blockids)
end

Schedulers.checkpoint_read!(checkpoint::AzStorage.AzObject, buffer, offset) = (read!(checkpoint, buffer; offset); nothing)
# each copy of a chunk is a block of its own; the commit keeps the chosen copy of each chunk, and Azure discards the other blocks
Schedulers.checkpoint_write_chunk(checkpoint::AzStorage.AzObject, ichunk, nchunks, buffer, offset, copy) = putblock(checkpoint, Schedulers.chunk_blockid(ichunk, copy), buffer)
Schedulers.checkpoint_commit(checkpoint::AzStorage.AzObject, copies) = putblocklist(checkpoint, [Schedulers.chunk_blockid(ichunk, copy) for (ichunk, copy) in enumerate(copies)])
# blocks that are not committed are discarded with the blob, or by Azure a week after the last one was written
Schedulers.checkpoint_abort(checkpoint::AzStorage.AzObject) = nothing

end
