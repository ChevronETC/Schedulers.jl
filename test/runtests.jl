using Distributed, Logging, Random, Schedulers, Test, Logging

ENV["JULIA_WORKER_TIMEOUT"] = "120"

function safe_addprocs(n)
    try
        length(addprocs(n))
    catch e
        Schedulers.logerror(e)
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
    epmap(foo1, 1:10, s; epmap_nworkers=()->nprocs()-1)

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

    epmap(foo2, 1:100, s; epmap_maxworkers=10, epmap_minworkers=10, epmap_nworkers=()->nprocs()-1)

    h = Dict()
    for w in workers()
        h[w] = 0
    end

    @test nworkers() > 5
    rmprocs(workers())

    for tsk = 1:100
        r = read(joinpath(tempdir(), "task-$s-$tsk.txt"), String)
        r_tsk, r_pid = split(r, ",")
        h[parse(Int,r_pid)] += 1
        @test r_tsk == "$tsk"
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end

    for (key,value) in h
        @test value ∈ 0:30
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
    tsk = @async epmap(foo3, 1:100, s; epmap_maxworkers=10, epmap_nworkers=()->nprocs()-1)

    sleep(15)
    faulty_pids = workers()[randperm(length(workers()))[1:2]]
    rmprocs(faulty_pids)

    wait(tsk)
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

    epmap(foo4, 1:105, s; epmap_maxworkers=10, epmap_minworkers=4, epmap_nworkers=()->nprocs()-1)

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

    tsk = @async epmap(foo5, 1:100, s; epmap_maxworkers=()->_nworkers, epmap_minworkers=()->_nworkers, epmap_nworkers=()->nprocs()-1)

    sleep(15)
    _nworkers = 10

    wait(tsk)
    @test nworkers() > 5

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
        @test value ∈ 0:20
    end
end

@testset "pmap with blocking addprocs" begin
    using Distributed, Schedulers

    s = randstring(6)
    function foo5b(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
        sleep(1)
    end

    i = 0
    function myaddprocs(n)
        i += 1
        @info "nworkers()=$(nworkers()), i=$i"
        if i == 1
            addprocs(n, "foo")
        elseif i == 5
            sleep(9999999) # block
        else
            sleep(2)
            addprocs(n)
        end
    end

    epmap(foo5b, 1:100, s; epmap_maxworkers=5, epmap_addprocs=myaddprocs, epmap_quantum=1, epmap_nworkers=()->nprocs()-1)

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
        @test value ∈ 10:50
    end
end

@testset "pmapreduce, stable cluster test, backwards compatability" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere function foo6(x, tsk, a; b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        nothing
    end

    tmpdir = mktempdir(;cleanup=false)

    a,b = 2,3
    x = epmapreduce!(zeros(Float32,10), foo6, 1:100, a; b=b, epmap_scratch=tmpdir, epmap_nworkers=()->nprocs()-1)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith("checkpoint", file), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, stable cluster test" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere function foo6(x, tsk, a; b)
        x .+= a*b*tsk
        nothing
    end

    tmpdir = mktempdir(;cleanup=false)

    a,b = 2,3
    x = epmapreduce!(zeros(Float32,10), foo6, 1:100, a; b=b, epmap_scratch=tmpdir, epmap_nworkers=()->nprocs()-1)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith("checkpoint", file), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with ProcessExitedException during tasks" begin
    safe_addprocs(5)
    wrkrs = workers()
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo7(x, tsk, a, b)
        x .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    tsk = @async epmapreduce!(zeros(Float32,10), foo7, 1:100, a, b; epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_nworkers=()->nprocs()-1)

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

    @everywhere function test_save_checkpoint(checkpoint, localresult, ::Type{T}) where {T}
        x = rand()
        if x > 0.8
            error("foo,x=$x")
        end
        Schedulers.default_save_checkpoint(checkpoint, localresult, T)
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    x = epmapreduce!(zeros(Float32,10), foo7b, 1:100, a, b; epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_save_checkpoint = test_save_checkpoint, epmap_retries=0, epmap_nworkers=()->nprocs()-1)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, force overlap of map and reduce" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere wrkrs = workers()

    s = randstring(6)
    @everywhere function foo7c(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        if tsk ∈ (97,98,99,100)
            sleep(60) # this should force the reduction to start before all tasks are finished.
        else
            sleep(1)
        end
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    x = epmapreduce!(zeros(Float32,10), foo7c, 1:100, a, b; epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_retries=0, epmap_nworkers=()->nprocs()-1)

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

        x .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    _pid = workers()[randperm(nworkers())[1]]
    toggle = remotecall_wait(()->[true], _pid)
    x = epmapreduce!(zeros(Float32,10), foo8, 1:10, a, b, toggle, _pid; epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_retries=1, epmap_maxerrors=Inf, epmap_nworkers=()->nprocs()-1)

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

        x .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    _pid = workers()[randperm(nworkers())[1]]
    toggle = remotecall_wait(()->[true], _pid)
    @test_throws Exception epmapreduce!(zeros(Float32,10), foo9, 1:10, a, b, toggle, _pid; epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_retries=1, epmap_maxerrors=1, epmap_nworkers=()->nprocs()-1)
    rmprocs(workers())

    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with ProcessExitedException during reduce" begin
    function foo9b(x, tsk, a, b)
        x .+= a*b*tsk
        sleep(1)
        nothing
    end

    function myreducer!(x, y)
        r = rand()
        @info "myreducer, r=$r"
        if r > 0.5
            pids = randperm(nworkers())
            if pids[1] != 1
                @info "removing process $(pids[1])"
                remotecall_fetch(rmprocs, 1, pids[1])
            end
        end
        x .+= y
        nothing
    end

    a,b = 2,3
    tmpdir = mktempdir(;cleanup=false)
    x = epmapreduce!(zeros(Float32,10), foo9b, 1:10, a, b, epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_reducer! = myreducer!, epmap_nworkers=()->nprocs()-1)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:10;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true)
end

@testset "pmapreduce, cluster with RemoteException during reduce" begin
    function foo9c(x, tsk, a, b)
        x .+= a*b*tsk
        sleep(1)
        nothing
    end

    function myreducer!(x, y)
        r = rand()
        R = 0.5
        if r > R && myid() != 1
            error("this is an error because $r is greater than $R")
        end
        x .+= y
        nothing
    end

    a,b = 2,3
    tmpdir = mktempdir(;cleanup=false)
    x = epmapreduce!(zeros(Float32,10), foo9c, 1:10, a, b, epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_reducer! = myreducer!, epmap_nworkers=()->nprocs()-1)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:10;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true)
end

@testset "pmapreduce, cluster with RemoteException during delete checkpoints" begin
    function foo9d(x, tsk, a, b)
        x .+= a*b*tsk
        sleep(1)
        nothing
    end

    function myrm(checkpoint)
        r = rand()
        R = 0.5
        if r > R && myid() != 1
            error("this is an error because $r is greater than $R")
        end
        isfile(checkpoint) && rm(checkpoint)
        nothing
    end

    a,b = 2,3
    tmpdir = mktempdir(;cleanup=false)
    x = epmapreduce!(zeros(Float32,10), foo9d, 1:10, a, b, epmap_maxworkers=5, epmap_scratch=tmpdir, epmap_rm_checkpoint = myrm, epmap_nworkers=()->nprocs()-1)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:10;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true)
end

@testset "pmapreduce, growing cluster test" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random, Test
    s = randstring(6)
    @everywhere function foo12(x, tsk, a, b)
        x .+= a*b*tsk
        sleep(5)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    x = epmapreduce!(zeros(Float32,10), foo12, 1:100, a, b; epmap_minworkers=5, epmap_maxworkers=11, epmap_scratch=tmpdir, epmap_addprocs=safe_addprocs, epmap_nworkers=()->nprocs()-1)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)

    @test mapreduce(file->startswith("checkpoint", file), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, interactive growing cluster test" begin
    safe_addprocs(5)
    sleep(2)
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo13(x, tsk, a, b)
        x .+= a*b*tsk
        sleep(5)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    _nworkers = 5

    local x
    tsk = @async epmapreduce!(zeros(Float32,10), foo13, 1:100, a, b; epmap_maxworkers=()->_nworkers, epmap_scratch=tmpdir, epmap_nworkers=()->nprocs()-1)

    sleep(20)
    _nworkers = 10
    sleep(20)
    @test nworkers() > 5

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
        x.y .+= a*tsk
        x.z .+= b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    my_zeros() = (y=zeros(Float32,10),z=zeros(Float32,10))
    x = my_zeros()

    epmapreduce!(x, foo14, 1:100, a, b;
        epmap_nworkers=()->nprocs()-1,
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
        x .+= a*b*tsk
        nothing
    end

    tmpdirs = [mktempdir(;cleanup=false) for i=1:3]

    a,b = 2,3
    x = epmapreduce!(zeros(Float32,10), foo15, 1:100, a; b=b, epmap_scratch=tmpdirs, epmap_keepcheckpoints=true, epmap_nworkers=()->nprocs()-1)

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
    try
        notafunction()
    catch e
        io = IOBuffer()
        with_logger(ConsoleLogger(io, Logging.Info)) do
            Schedulers.logerror(e, Logging.Warn)
        end
        s = String(take!(io))
        @test contains(s, "notafunction")
    end
end
