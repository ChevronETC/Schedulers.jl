using AzManagers, Distributed, MPI, Random
n = 1
addprocs("cbox08", n; group="lvjn-mpitest-$(randstring(3))", mpi_ranks_per_worker=2)

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

result1 = remotecall_fetch(my_method, workers()[1], 1) # will get through this remotecall successfully
@show result1
result2 = remotecall_fetch(my_method, workers()[1], 2) # will hang at MPI barrier for this remotecall
@show result2
rmprocs(workers())

