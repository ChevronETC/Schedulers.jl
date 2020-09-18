using Distributed, Random, Test

@testset "pmap, stable cluster test" begin
    addprocs(5)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo1(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk,$(myid())")
        sleep(10)
    end
    epmap(foo1, 1:10, s)

    h = Dict()
    for w in workers()
        h[w] = 0
    end

    @test nworkers() == 5
    rmprocs(workers())

    for tsk = 1:10
        r = read(joinpath(tempdir(), "task-$s-$tsk.txt"), String)
        r_tsk, r_pid = split(r, ",")
        h[parse(Int,r_pid)] += 1
        @test r_tsk == "$tsk"
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end

    for (key,value) in h
        @test value ∈ 0:4
    end
end

@testset "pmap, growing cluster test" begin
    addprocs(2)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo2(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
        sleep(10)
    end

    epmap(foo2, 1:100, s; epmap_maxworkers=10, epmap_minworkers=10)

    h = Dict()
    for w in workers()
        h[w] = 0
    end

    rmprocs(workers())

    for tsk = 1:100
        r = read(joinpath(tempdir(), "task-$s-$tsk.txt"), String)
        r_tsk, r_pid = split(r, ",")
        h[parse(Int,r_pid)] += 1
        @test r_tsk == "$tsk"
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end

    for (key,value) in h
        @test value ∈ 5:25
    end
end

@testset "pmap, elastic cluster with faults" begin
    addprocs(10)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo3(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
        sleep(10)
    end
    tsk = @async epmap(foo3, 1:100, s; epmap_maxworkers=10)

    sleep(15)
    faulty_pids = workers()[randperm(length(workers()))[1:2]]
    rmprocs(faulty_pids)

    wait(tsk)
    @test nworkers() == 10
    w = workers()

    h = Dict()

    rmprocs(workers())

    for tsk = 1:100
        r = read(joinpath(tempdir(), "task-$s-$tsk.txt"), String)
        r_tsk, r_pid = split(r, ",")
        if haskey(h, parse(Int, r_pid))
            h[parse(Int,r_pid)] += 1
        else
            h[parse(Int,r_pid)] = 1
        end
        @test r_tsk == "$tsk"
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end

    for (key,value) in h
        if key ∈ workers()
            @test value ∈ 5:25
        end
    end
end

@testset "pmap with shrinking cluster" begin
    addprocs(10)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo3(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
        sleep(10)
    end

    h = Dict()
    for w in workers()
        h[w] = 0
    end

    epmap(foo3, 1:105, s; epmap_maxworkers=10, epmap_minworkers=4)

    @test nworkers() == 4
    rmprocs(workers())

    for tsk = 1:105
        r = read(joinpath(tempdir(), "task-$s-$tsk.txt"), String)
        r_tsk, r_pid = split(r, ",")
        h[parse(Int,r_pid)] += 1
        @test r_tsk == "$tsk"
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end

    for (key,value) in h
        @test value ∈ 5:25
    end
end

@testset "pmapreduce, stable cluster test" begin
    addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere function foo4(x, tsk, a; b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        nothing
    end

    tmpdir = mktempdir()

    a,b = 2,3
    x = epmapreduce!(zeros(Float32,10), foo4, 1:100, a; b=b, epmap_scratch=tmpdir)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith("checkpoint", file), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with faults during tasks" begin
    # TODO, how do we excersie the differnt fault mechanisms in the task loop
    #       1. fault during a f eval
    #       2. fault during checkpoint
    #       3. fault when removing old checkpoint
    #       4. fault when reducing-in an ophan
    #       5. fault when checkpoiting the reduced-in result
    #       6. fault when removing old orphaned checkpoints
    addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo4(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir()

    tsk = @async epmapreduce!(zeros(Float32,10), foo4, 1:100, a, b; epmap_scratch=tmpdir)

    sleep(10)
    rmprocs(workers()[randperm(nworkers())[1]])

    x = fetch(tsk)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with faults during reduce" begin
    # TODO, how do we excersie the differnt fault mechanisms in the task loop
    #       1. fault during reduce
    #       2. fault during removal of checkpoint1
    #       3. fault during removal of checkpoint2

    addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo5(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3
    id = randstring(6)

    tmpdir = mktempdir()

    checkpoints = Schedulers.epmapreduce_map(foo5, 1:100, Float32, (10,), a, b;
        epmapreduce_id=id, epmap_minworkers=nworkers(), epmap_maxworkers=nworkers(), epmap_addprocs=Schedulers.epmap_default_addprocs, epmap_quantum=32, epmap_scratch=tmpdir)

    tsk = @async Schedulers.epmapreduce_reduce!(zeros(Float32,10), checkpoints;
        epmapreduce_id=id, epmap_minworkers=nworkers(), epmap_maxworkers=nworkers(), epmap_addprocs=Schedulers.epmap_default_addprocs, epmap_quantum=32, epmap_scratch=tmpdir)

    rm(tmpdir; recursive=true, force=true)
    rmprocs(workers()[randperm(nworkers())[1]])

    x = fetch(tsk)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:100;])*ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, growing cluster test" begin
    addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo4(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(5)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir()

    x = epmapreduce!(zeros(Float32,10), foo4, 1:100, a, b; epmap_maxworkers=10)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)

    @test mapreduce(file->startswith("checkpoint", file), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end