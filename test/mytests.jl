using AzManagers, AzStorage, AzSessions, Distributed, Logging, Random, Schedulers, Serialization, Test, MPI, MFWIs

function safe_addprocs(n, prefix)
    try
        machine_type = "cbox08"
        group = prefix
        mpi_ranks_per_worker = 2
        addprocs(machine_type, n; 
                group=group, customenv=true, 
                mpi_ranks_per_worker=mpi_ranks_per_worker)
    catch e
        Schedulers.logerror(e)
        @warn "problem calling addprocs, nworkers=$(nworkers())"
    end
end

session_scalesets = AzSession(;
    scope_auth="openid+offline_access+https://management.azure.com/user_impersonation+https://storage.azure.com/user_impersonation",
    scope = "openid+offline_access+https://management.azure.com/user_impersonation")

session_storage = AzSession(session_scalesets; scope="openid+offline_access+https://storage.azure.com/user_impersonation")
storageaccount = "ctcrd"
container_path = "lvjn/test"
prefix = "lvjn-test-$(randstring(3))"

function init_worker(pid)
    MPI.Init()
    nranks = MPI.Comm_size(MPI.COMM_WORLD)
    @show "Initializing $pid $nranks"
    nothing
end

function my_zeros()
    MPI.Init()
    nranks = MPI.Comm_size(MPI.COMM_WORLD)
    @show "Making Zeros! $nranks"
    # initial_zeros =  (MPI.Comm_rank(MPI.COMM_WORLD) == 0 ? zeros(Float32,10) : nothing)
    initial_zeros = zeros(Float32,10)
    MPI.Barrier(MPI.COMM_WORLD)
    @show "Done with Zeros! $nranks"
    return initial_zeros
end

function my_save_checkpoint(checkpoint, localresult, ::Type{T}) where {T} 
    MPI.Init()
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

function my_load_checkpoint(checkpoint, ::Type{T}) where {T}
    MPI.Init()
    @show "Loading Checkpoint! $nranks"
    if MPI.Comm_rank(MPI.COMM_WORLD) == 0 
        loaded = deserialize(checkpoint::T)
    else
        loaded = nothing
    end
    @show "Through DeSerialization $nranks"
    MPI.Barrier(MPI.COMM_WORLD)
    @show "Done Loading Checkpoint! $nranks"
    return loaded
end

@testset "pmapreduce, stable cluster test, backwards compatability" begin
    N = 2 
    scratch = AzContainer("$(container_path)/$prefix/scratch"; storageaccount=storageaccount, session=session_storage)
    journalfile = "testjournal-$prefix.json"

    a,b = 2,3
    options = SchedulerOptions(;scratch=scratch, 
                                journalfile=journalfile, 
                                addprocs=n->safe_addprocs(n, prefix),
                                quantum=5,
                                init = pid->init_worker(pid),
                                nworkers=nworkers_provisioned,
                                minworkers=1,
                                maxworkers=N,
                                zeros=my_zeros,
                                epmapreduce_fetch_apply=MFWIs.my_fetch_apply,
                                save_checkpoint=my_save_checkpoint,
                                load_checkpoint=my_load_checkpoint
                                )
    x = epmapreduce!(zeros(Float32,10), options, MFWIs.foo6mpi, 1:N, a, b)

    rmprocs(workers())
    @test x ≈ sum(a*b*[1:N;]) * ones(10)

end