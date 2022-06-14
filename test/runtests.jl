using Distributed, Logging, Random, Test, Logging

function safe_addprocs(n)
    try
        length(addprocs(n))
    catch
        @warn "problem calling addprocs, nworkers=$(nworkers())"
    end
end

@testset "pmap, stable cluster test" begin
    safe_addprocs(5)
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
    safe_addprocs(2)
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

    @test nworkers() == 10
    rmprocs(workers())

    for tsk = 1:100
        r = read(joinpath(tempdir(), "task-$s-$tsk.txt"), String)
        r_tsk, r_pid = split(r, ",")
        h[parse(Int,r_pid)] += 1
        @test r_tsk == "$tsk"
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end

    for (key,value) in h
        @test value ∈ 5:15
    end
end

@testset "pmap, elastic cluster with faults" begin
    safe_addprocs(10)
    wrkrs = workers()
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
    _wrkrs = workers()
    @test wrkrs != _wrkrs

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
        if key ∈ _wrkrs
            @test value ∈ 5:15
        end
    end
end

@testset "pmap with shrinking cluster" begin
    safe_addprocs(10)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo4(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
        sleep(10)
    end

    h = Dict()
    for w in workers()
        h[w] = 0
    end

    epmap(foo4, 1:105, s; epmap_maxworkers=10, epmap_minworkers=4)

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
        @test value ∈ 5:15
    end
end

@testset "pmap with interactive growing cluster" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo5(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
        sleep(10)
    end

    _nworkers = 5

    tsk = @async epmap(foo5, 1:100, s; epmap_maxworkers=()->_nworkers, epmap_minworkers=()->_nworkers)

    sleep(15)
    _nworkers = 10

    wait(tsk)
    @test nworkers() == 10

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
        @test value ∈ 5:15
    end
end

@testset "pmapreduce, stable cluster test" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere function foo6(x, tsk, a; b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        nothing
    end

    tmpdir = mktempdir(;cleanup=false)

    a,b = 2,3
    x = epmapreduce!(zeros(Float32,10), foo6, 1:100, a; b=b, epmap_scratch=tmpdir)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith("checkpoint", file), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with ProcessExitedException during tasks" begin
    # TODO, how do we excersie the different fault mechanisms in the task loop
    #       1. fault during a f eval
    #       2. fault when removing old checkpoint
    #       3. fault when reducing-in an ophan
    #       4. fault when checkpoiting the reduced-in result
    #       5. fault when removing old orphaned checkpoints
    safe_addprocs(5)
    wrkrs = workers()
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo7(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    tsk = @async epmapreduce!(zeros(Float32,10), foo7, 1:100, a, b; epmap_maxworkers=5, epmap_scratch=tmpdir)

    sleep(10)
    rmprocs(workers()[randperm(nworkers())[1]])

    x = fetch(tsk)
    @test nworkers() == 5
    _wrkrs = workers()
    @test wrkrs != _wrkrs

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with ErrorException during checkpoint" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere wrkrs = workers()

    s = randstring(6)
    @everywhere function foo7b(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(1)
        nothing
    end

    flg = remotecall_wait(()->[true], workers()[1])
    
    @everywhere function test_save_checkpoint(checkpoint, localresult, ::Type{T}) where {T}
        x = rand()
        if x > 0.8
            error("foo,x=$x")
        end
        Schedulers.default_save_checkpoint(checkpoint, localresult, T)
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    x = epmapreduce!(zeros(Float32,10), foo7b, 1:100, a, b; epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_save_checkpoint = test_save_checkpoint, epmap_retries=0)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with RemoteException during tasks" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo8(x, tsk, a, b, toggle, fault_id)
        _toggle = fetch(toggle)
        if myid() == fault_id && _toggle[1]
            _toggle[1] = false
            error("throwing an error")
        end

        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    _pid = workers()[randperm(nworkers())[1]]
    toggle = remotecall_wait(()->[true], _pid)
    x = epmapreduce!(zeros(Float32,10), foo8, 1:10, a, b, toggle, _pid; epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_retries=1, epmap_maxerrors=Inf)

    @test nworkers() == 5
    rmprocs(workers())

    @test x ≈ sum(a*b*[1:10;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with RemoteException during tasks, and max errors triggered" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo9(x, tsk, a, b, toggle, fault_id)
        _toggle = fetch(toggle)
        if myid() == fault_id && _toggle[1]
            _toggle[1] = false
            error("throwing an error")
        end

        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    _pid = workers()[randperm(nworkers())[1]]
    toggle = remotecall_wait(()->[true], _pid)
    @test_throws Exception epmapreduce!(zeros(Float32,10), foo9, 1:10, a, b, toggle, _pid; epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_retries=1, epmap_maxerrors=1)
    rmprocs(workers())

    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with ProcessExitedException during reduce" begin ### whoop!!!
    # TODO, how do we excersie the differnt fault mechanisms in the task loop
    #       1. fault during reduce
    #       2. fault during removal of checkpoint1
    #       3. fault during removal of checkpoint2
    safe_addprocs(5)
    __workers = workers()
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo10(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3
    id = randstring(6)

    tmpdir = mktempdir(;cleanup=false)

    epmap_zeros = ()->zeros(Float32,10)

    result = epmap_zeros()

    journal = Schedulers.journal_init(1:100, tsks->nothing; reduce=true)

    eloop = Schedulers.ElasticLoop(;
        epmap_init = Schedulers.epmap_default_init,
        epmap_addprocs = Schedulers.epmap_default_addprocs,
        epmap_quantum = ()->32,
        epmap_minworkers = nworkers(),
        epmap_maxworkers = nworkers(),
        epmap_nworkers = nworkers,
        epmap_usemaster = false,
        tasks = 1:100,
        exit_on_empty = false,
        run_init = true)

    _elastic_loop = @async Schedulers.loop(eloop)

    checkpoints = Schedulers.epmapreduce_map(foo10, result, eloop, journal, a, b;
        epmap_save_checkpoint = Schedulers.default_save_checkpoint,
        epmapreduce_id = id,
        epmap_accordion = false,
        epmap_reducer! = Schedulers.default_reducer!,
        epmap_zeros = epmap_zeros,
        epmap_preempted = Schedulers.epmap_default_preempted,
        epmap_scratches = [tmpdir],
        epmap_reporttasks = true,
        epmap_maxerrors = Inf,
        epmap_retries = 0,
        epmap_keepcheckpoints = false,
        epmap_journal_task_callback = tsk->nothing)

    empty!(eloop, [1:length(checkpoints)-1;])
    eloop.exit_on_empty = true
    eloop.run_init = false

    tsk = @async Schedulers.epmapreduce_reduce!(result, checkpoints, eloop, journal;
        epmapreduce_id = id,
        epmap_reducer! = Schedulers.default_reducer!,
        epmap_preempted = Schedulers.epmap_default_preempted,
        epmap_scratches = [tmpdir],
        epmap_reporttasks = true,
        epmap_maxerrors = Inf,
        epmap_retries = 0,
        epmap_keepcheckpoints = false)

    rmprocs(workers()[randperm(nworkers())[1]])

    fetch(_elastic_loop)
    _workers = workers()
    @test _workers != __workers
    1 ∈ _workers && popfirst!(_workers)
    rmprocs(_workers[1:(length(_workers) - eloop.epmap_minworkers())])

    x = fetch(tsk)

    Schedulers.journal_final(journal)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:100;])*ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with RemoteException during reduce" begin
    # TODO, how do we excersie the different fault mechanisms in the task loop
    #       1. fault during reduce
    #       2. fault during removal of checkpoint1
    #       3. fault during removal of checkpoint2

    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo11(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3
    id = randstring(6)

    tmpdir = mktempdir(;cleanup=false)

    _pid = workers()[randperm(nworkers())[1]]
    toggle = remotecall_wait(()->[true], _pid)

    @everywhere function myreducer(x, y, toggle, _pid)
        _toggle = fetch(toggle)
        if _toggle[1] && _pid == myid()
            _toggle[1] = false
            error("this is an error")
        end
        x .+= y
        nothing
    end

    x = epmapreduce!(zeros(Float32,10), foo11, 1:20, a, b;
        epmap_reducer! = (x,y)->myreducer(x,y,toggle,_pid),
        epmap_maxworkers = 5,
        epmap_scratch = tmpdir,
        epmap_retries = 1,
        epmap_maxerrors = Inf)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:20;])*ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, growing cluster test" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random, Test
    s = randstring(6)
    @everywhere function foo12(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        if tsk == 100
            sleep(20)
        else
            sleep(5)
        end
        if tsk == 100
            @test nworkers() < 11
        end
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    x = epmapreduce!(zeros(Float32,10), foo12, 1:100, a, b; epmap_minworkers=5, epmap_maxworkers=11, epmap_scratch=tmpdir)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)

    @test mapreduce(file->startswith("checkpoint", file), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, growing cluster test, no accordion" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random, Test
    s = randstring(6)
    @everywhere function foo12(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        if tsk == 100
            sleep(20)
        else
            sleep(5)
        end
        if tsk == 100
            @test nworkers() == 11
        end
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    x = epmapreduce!(zeros(Float32,10), foo12, 1:100, a, b; epmap_minworkers=5, epmap_maxworkers=11, epmap_scratch=tmpdir, epmap_accordion=false)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)

    @test mapreduce(file->startswith("checkpoint", file), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, interactive growing cluster test" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo13(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(5)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    _nworkers = 5

    local x
    tsk = @async epmapreduce!(zeros(Float32,10), foo13, 1:100, a, b; epmap_maxworkers=()->_nworkers, epmap_scratch=tmpdir)

    sleep(20)
    _nworkers = 10
    sleep(20)
    @test nworkers() == 10

    x = fetch(tsk)

    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)

    @test mapreduce(file->startswith("checkpoint", file), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, structured data test" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo14(x, tsk, a, b)
        fetch(x).y::Vector{Float32} .+= a*tsk
        fetch(x).z::Vector{Float32} .+= b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    my_zeros() = (y=zeros(Float32,10),z=zeros(Float32,10))
    x = my_zeros()

    epmapreduce!(x, foo14, 1:100, a, b;
        epmap_maxworkers = 10,
        epmap_scratch = tmpdir,
        epmap_zeros = my_zeros,
        epmap_reducer! = (x,y)->(x.y .+= y.y; x.z .+= y.z; nothing))

    rmprocs(workers())

    @test x.y ≈ sum([a*tsk for tsk in 1:100])*ones(Float32,10)
    @test x.z ≈ sum([b*tsk for tsk in 1:100])*ones(Float32,10)
end

@testset "pmapreduce, multiple scratch locations" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere function foo15(x, tsk, a; b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        nothing
    end

    tmpdirs = [mktempdir(;cleanup=false) for i=1:3]

    a,b = 2,3
    x = epmapreduce!(zeros(Float32,10), foo15, 1:100, a; b=b, epmap_scratch=tmpdirs, epmap_keepcheckpoints=true)

    ncheckpoints = [length(readdir(tmpdir)) for tmpdir in tmpdirs]
    ncheckpoints_average = sum(ncheckpoints) / 3
    for i = 1:3
        @test ncheckpoints[i] > 0
        @test (ncheckpoints[i] - ncheckpoints_average) < .1*ncheckpoints_average
    end

    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    rm.(tmpdirs; recursive=true, force=true)
end

@testset "logerror" begin
    using Schedulers
    try
        notafunction()
    catch e
        io = IOBuffer()
        with_logger(ConsoleLogger(io, Logging.Info)) do
            Schedulers.logerror(e)
        end
        s = String(take!(io))
        @test contains(s, "notafunction")
    end
end
