using AzManagers, Distributed, MPI, Random, AzSessions, AzStorage
n = 1

prefix = "lvjn-mpitest-$(randstring(3))"
addprocs("cbox08", n; group=prefix, mpi_ranks_per_worker=2)
session_scalesets = AzSession(;
    scope_auth="openid+offline_access+https://management.azure.com/user_impersonation+https://storage.azure.com/user_impersonation",
    scope = "openid+offline_access+https://management.azure.com/user_impersonation")

session_storage = AzSession(session_scalesets; scope="openid+offline_access+https://storage.azure.com/user_impersonation")
storageaccount = "ctcrd"
container_path = "lvjn/test"

scratch = AzContainer("$(container_path)/$prefix/scratch";  storageaccount=storageaccount, session=session_storage)
scratch = isa(scratch, AbstractArray) ? scratch : [scratch]
for scr in scratch
    isdir(scr) || mkpath(scr)
end
id = randstring(6)

while workers()[1] == 1
    @info "Not yet provisioned, sleeping"
    sleep(5)
end

@everywhere function my_method(inpt)
    MPI.Init()
    @show "into method for input $inpt"
    mynumber = (MPI.Comm_rank(MPI.COMM_WORLD) + 1) * inpt
    MPI.Barrier(MPI.COMM_WORLD)
    @show "barrier done for input $inpt"
    return mynumber
end

@everywhere function my_zeros()
    MPI.Init()
    nranks = MPI.Comm_size(MPI.COMM_WORLD)
    @show "Making Zeros! $nranks"
    # initial_zeros =  (MPI.Comm_rank(MPI.COMM_WORLD) == 0 ? zeros(Float32,10) : nothing)
    initial_zeros = zeros(Float32,10)
    MPI.Barrier(MPI.COMM_WORLD)
    @show "Done with Zeros! $nranks"
    return initial_zeros
end

@everywhere function my_fetch_apply(_localresult, ::Type{T}, f, itsk, args...; kwargs...) where {T}
    @show "into fetch apply"
    MPI.Init()
    MPI.Barrier(MPI.COMM_WORLD)
    @show "through barrier on fetch apply"
    localresult = (MPI.Comm_rank(MPI.COMM_WORLD) == 0 ? fetch(_localresult) : nothing)
    f(localresult, itsk, args...; kwargs...)
    MPI.Barrier(MPI.COMM_WORLD)
    @show "at end barrier on fetch apply"
    nothing
end


@everywhere function foo9mpi(x, tsk, a; b)
    MPI.Init()
    comm = MPI.COMM_WORLD
    myrank = MPI.Comm_rank(comm)
    mynumber = nothing
    if myrank > 0
        mynumber = myrank
    end
    @show mynumber
    mynumber = MPI.bcast(mynumber, 1, comm)
    @show mynumber
    if myrank == 0 
        x .+= a*b*tsk*mynumber
    end
    MPI.Barrier(MPI.COMM_WORLD)
    @show x
    nothing
end

@everywhere using Schedulers, AzStorage

@everywhere my_preempted = ()->false


@everywhere function my_save_checkpoint_with_timeout(f, checkpoint, localresult, ::Type{T}, latency, throughput) where {T}
    tsk = @async f(checkpoint, localresult, T)

    timeout = latency + length(fetch(localresult)::T) / throughput
    tic = time()
    while !(istaskdone(tsk))
        if time() - tic > timeout
            break;
        end
        sleep(1)
    end
    if !(istaskdone(tsk))
        @show "into interupt!"
        @async Base.throwto(tsk, InterruptException())
        @show "trying to throw timeout exception"
        throw(SaveCheckpointTimeoutException(myid(), round(Int,timeout), checkpoint))
    end
    nothing
    # @show "into fetch"
    # my_result = fetch(tsk)
    # @show "fetched checkpoint result"
    # my_result
end


@everywhere function my_save_checkpoint(checkpoint, localresult, ::Type{T}) where {T} 
    MPI.Initialized() || MPI.Init()
    nranks = MPI.Comm_size(MPI.COMM_WORLD)
    @show "Saving Checkpoint! $nranks"
    if MPI.Comm_rank(MPI.COMM_WORLD) == 0 
        serialize(checkpoint, fetch(localresult)::T)
        @show "Through Serialization $nranks"
    end
    MPI.Barrier(MPI.COMM_WORLD)
    @show "Done Saving Checkpoint! $nranks"
    nothing
end


# result1 = remotecall_fetch(MFWIs.foo9mpi, workers()[1], 1) # will get through this remotecall successfully
# @show result1
# result2 = remotecall_fetch(MFWIs.foo9mpi, workers()[1], 2) # will hang at MPI barrier for this remotecall
# @show result2
# result3 = remotecall_wait( MFWIs.foo9mpi, workers()[1], 3)
# result4 = remotecall_fetch( MFWIs.foo9mpi, workers()[1], 4)
pid = workers()[1]
tsk = 1
a = 2
b = 3
args = (a,)
kwargs = (b=b,)
preempted = remotecall_fetch(my_preempted, pid)
@show preempted
hostname = remotecall_fetch(gethostname, pid)
@show hostname
localresult = remotecall(my_zeros, pid)
T = typeof(zeros(Float32,10))
remotecall_wait(my_fetch_apply, pid, localresult, T, foo9mpi, tsk, args...; kwargs...)
_next_checkpoint = Schedulers.next_checkpoint(id, scratch)
remotecall_wait(Schedulers.save_checkpoint_with_timeout, pid, my_save_checkpoint, _next_checkpoint, localresult, T, 9999, 1)
hostname = remotecall_fetch(gethostname, pid)
@show hostname

rmprocs(workers())

