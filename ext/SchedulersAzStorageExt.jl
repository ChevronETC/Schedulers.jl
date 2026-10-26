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
Schedulers.checkpoint_write_chunk(checkpoint::AzStorage.AzObject, ichunk, nchunks, buffer, offset) = putblock(checkpoint, AzStorage.blockids(nchunks)[ichunk], buffer)
Schedulers.checkpoint_commit(checkpoint::AzStorage.AzObject, nchunks) = putblocklist(checkpoint, AzStorage.blockids(nchunks))
Schedulers.checkpoint_abort(checkpoint::AzStorage.AzObject) = nothing

end
