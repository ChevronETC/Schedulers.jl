using Distributed, Logging, Random, Schedulers, Test

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

    options = SchedulerOptions(;maxworkers=10,minworkers=10)
    epmap(options, foo2, 1:100, s)

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
    options = SchedulerOptions(;maxworkers=10)
    tsk = @async epmap(options, foo3, 1:100, s)

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

    options = SchedulerOptions(;maxworkers=10, minworkers=4)
    epmap(options, foo4, 1:105, s)

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

    options = SchedulerOptions(;maxworkers=()->_nworkers, minworkers=()->_nworkers)
    tsk = @async epmap(options, foo5, 1:100, s)

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

    options = SchedulerOptions(;maxworkers=5, addprocs=myaddprocs, quantum=1)
    epmap(options, foo5b, 1:100, s)

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

@testset "pmap with timeout" begin
    using Distributed, Schedulers

    s = randstring('a':'z', 4)

    function foo5c(tsk, s)
        if tsk == 20 && !isfile(joinpath(tempdir(), "touch.txt"))
            write(joinpath(tempdir(), "touch.txt"), "touch")
            sleep(600)
        else
            sleep(1)
        end
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
    end

    options = SchedulerOptions(;maxworkers=5)
    journal,tsks = epmap(options, foo5c, 1:20, s)
    @test isempty(tsks)
    rm(joinpath(tempdir(), "touch.txt"))

    for tsk = 1:20
        @test isfile(joinpath(tempdir(), "task-$s-$tsk.txt"))
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end
end

@testset "pmap with timeout and skip_tasks_that_timeout=true" begin
    using Distributed, Schedulers

    s = randstring('a':'z', 4)

    function foo5c(tsk, s)
        if tsk == 20
            sleep(600)
        else
            sleep(1)
        end
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
    end

    options = SchedulerOptions(;maxworkers=5, skip_tasks_that_timeout=true)
    journal,tsks = epmap(options, foo5c, 1:20, s)
    @test tsks == [20]

    for tsk = 1:20
        if tsk == 20
            @test isfile(joinpath(tempdir(), "task-$s-$tsk.txt")) == false
        else
            @test isfile(joinpath(tempdir(), "task-$s-$tsk.txt"))
            rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
        end
    end
end

@testset "pmap with task checkpoint and restart" begin
    s = Dict{Int,Future}()
    p = Dict{Int,Future}()

    function init(pid, s)
        s[pid] = remotecall(ones, pid, Int, 1)
        p[pid] = remotecall(Channel{Bool}, pid, 1)
    end

    r = randstring('a':'z', 6)

    function foo5d(tsk, s, r)
        _s = fetch(s[myid()])::Vector{Int}
        for i = _s[1]:10
            _s .= i
            touch("testfile-$r-$tsk-$i.txt")
            @info "_s on pid=$(myid()) is $(_s[1])"
            sleep(5)
        end
        _s .= 1
    end

    function checkpoint_task(tsk, s)
        @info "checkpoint task..."
        _s = fetch(s[myid()])::Vector{Int}
        write("task_checkpoint_$tsk.bin", _s)
        @info "...checkpoint task with state=$(_s[1])."
    end

    function restart_task!(tsk, s)
        @info "restart task..."
        _s = fetch(s[myid()])::Vector{Int}
        if isfile("task_checkpoint_$tsk.bin")
            read!("task_checkpoint_$tsk.bin", _s)
            rm("task_checkpoint_$tsk.bin")
        end
        @info "...restart task, " _s
    end

    function signal_preempt(p)
        put!(fetch(p[myid()]), true)
    end

    options = SchedulerOptions(;
        minworkers = 0,
        maxworkers = 2,
        init = pid->init(pid, s),
        checkpoint_task = tsk->checkpoint_task(tsk, s),
        restart_task = tsk->restart_task!(tsk, s),
        preempt_channel_future = pid->p[pid]
    )

    t = @async epmap(options, i->foo5d(i,s,r), 1:4)

    sleep(25)
    remotecall_wait(signal_preempt, workers()[1], p)

    journal,tsks = fetch(t)
    files = filter(f->startswith(f, "testfile-$r"), readdir())
    @test length(files) == 40
    rm.(files)
end

@testset "pmapreduce, stable cluster test, backwards compatibility" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere function foo6(x, tsk, a; b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        nothing
    end

    tmpdir = mktempdir(;cleanup=false)

    a,b = 2,3
    options = SchedulerOptions(;scratch=tmpdir)
    options_init = deepcopy(options)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo6, 1:100, a; b=b)

    rmprocs(workers())
    for field ∈ fieldnames(SchedulerOptions)
        @test getfield(options_init, field) == getfield(options, field)
    end
    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, stable cluster test" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere function foo6b(x, tsk, a; b)
        x .+= a*b*tsk
        nothing
    end

    tmpdir = mktempdir(;cleanup=false)

    a,b = 2,3
    options = SchedulerOptions(;scratch=tmpdir)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo6b, 1:100, a; b=b)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
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

    options = SchedulerOptions(;maxworkers=5, scratch=tmpdir)
    tsk = @async epmapreduce!(zeros(Float32,10), options, foo7, 1:100, a, b)

    sleep(10)
    rmprocs(workers()[randperm(nworkers())[1]])

    x,tsks = fetch(tsk)
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

    @everywhere function test_save_checkpoint(checkpoint, localresult)
        x = rand()
        if x > 0.8
            error("foo,x=$x")
        end
        Schedulers.default_save_checkpoint(checkpoint, localresult)
    end

    @everywhere function test_load_checkpoint(checkpoint)
        x = rand()
        if x > 0.8
            error("bar,x=$x")
        end
        Schedulers.default_load_checkpoint(checkpoint)
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(;maxworkers=5, scratch=tmpdir, load_checkpoint=test_load_checkpoint, save_checkpoint = test_save_checkpoint, retries=0)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo7b, 1:100, a, b)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with ErrorException during checkpoint and retries=1" begin
    # important to test with retries=1 since we need to ensure that we don't reduce things twice
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere wrkrs = workers()

    s = randstring(6)
    @everywhere function foo7b(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(1)
        nothing
    end

    @everywhere function test_save_checkpoint(checkpoint, localresult)
        x = rand()
        if x > 0.8
            error("foo,x=$x")
        end
        Schedulers.default_save_checkpoint(checkpoint, localresult)
    end

    @everywhere function test_load_checkpoint(checkpoint)
        x = rand()
        if x > 0.8
            error("bar,x=$x")
        end
        Schedulers.default_load_checkpoint(checkpoint)
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(;maxworkers=5, scratch=tmpdir, load_checkpoint=test_load_checkpoint, save_checkpoint = test_save_checkpoint, retries=1)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo7b, 1:100, a, b)

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

    options = SchedulerOptions(;maxworkers=5, scratch=tmpdir, retries=0, timeout_function_multiplier=70)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo7c, 1:100, a, b)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with RemoteException during tasks" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    s = randstring(6)
    @everywhere function foo8(x, tsk, a, b)
        r = rand()
        if r > 0.9
            error("throwing a task error because $r is larger than 0.9")
        end

        x .+= a*b*tsk
        sleep(1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(;minworkers=5, maxworkers=5, scratch=tmpdir, retries=0, maxerrors=typemax(Int))
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo8, 1:10, a, b)

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
    options = SchedulerOptions(; maxworkers=5, scratch=tmpdir, retries=1, maxerrors=1)
    @test_throws Exception epmapreduce!(zeros(Float32,10), options, foo9, 1:10, a, b, toggle, _pid)
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
    options = SchedulerOptions(;maxworkers=5, scratch=tmpdir, reducer! = myreducer!)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo9b, 1:10, a, b)
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
    options = SchedulerOptions(;maxworkers=5, scratch=tmpdir, reducer! = myreducer!)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo9c, 1:10, a, b)
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
    options = SchedulerOptions(;maxworkers=5, scratch=tmpdir, rm_checkpoint = myrm)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo9d, 1:10, a, b)
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

    options = SchedulerOptions(; minworkers=5, maxworkers=11, scratch=tmpdir, addprocs=safe_addprocs)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo12, 1:100, a, b)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)

    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
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
    options = SchedulerOptions(; maxworkers=()->_nworkers, scratch=tmpdir, nworkers=()->nprocs()-1)
    tsk = @async epmapreduce!(zeros(Float32,10), options, foo13, 1:100, a, b)

    sleep(20)
    _nworkers = 10
    sleep(20)
    @test nworkers() > 5

    x,tsks = fetch(tsk)

    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)

    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
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

    options = SchedulerOptions(;maxworkers = 10, scratch = tmpdir, zeros = my_zeros, reducer! = (x,y)->(x.y .+= y.y; x.z .+= y.z; nothing))
    epmapreduce!(x, options, foo14, 1:100, a, b)

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
    options = SchedulerOptions(;scratch=tmpdirs, keepcheckpoints=true)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo15, 1:100, a; b=b)

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

@testset "pmapreduce, partial reduction" begin
    using Distributed, Schedulers

    function foo16(r, i)
        r .+= i
        sleep(5)
        nothing
    end

    function my_reduce_trigger(eloop, ntasks)
        if length(complete_tasks(eloop)) - ntasks[] > 50
            trigger_reduction!(eloop)
            ntasks[] = length(complete_tasks(eloop))
        end
    end

    tmpdir = mktempdir(;cleanup=false)
    tmpfile = tempname()

    ntasks = Ref(0)
    options = SchedulerOptions(;maxworkers=10, scratch=tmpdir, reduce_trigger=eloop->my_reduce_trigger(eloop, ntasks), save_partial_reduction=input->write(tmpfile, input))
    r,tsks = epmapreduce!(zeros(10), options, foo16, 1:100)

    @test r ≈ sum([1:100;]) * ones(10)

    x = read!(tmpfile, zeros(10))
    @test x[1] >= sum([1:50;])
    for i in eachindex(x)
        @test x[i] ≈ x[1]
    end

    rm(tmpfile)
    rm(tmpdir; recursive=true, force=true)
end

@testset "epmapreduce, timeout during f eval" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere function foo17(x, tsk, a; b)
        if tsk == 20 && !isfile(joinpath(tempdir(), "touch.txt"))
            write(joinpath(tempdir(), "touch.txt"), "touch")
            sleep(600)
        else
            sleep(0.1)
        end
        x .+= a*b*tsk
        nothing
    end

    tmpdirs = [mktempdir(;cleanup=false) for i=1:3]

    a,b = 2,3
    options = SchedulerOptions(;scratch=tmpdirs, keepcheckpoints=true)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo17, 1:100, a; b=b)
    rm(joinpath(tempdir(), "touch.txt"))
    @test isempty(tsks)

    ncheckpoints = [length(readdir(tmpdir)) for tmpdir in tmpdirs]
    ncheckpoints_average = sum(ncheckpoints) / 3
    for i = 1:3
        @test ncheckpoints[i] > 0
        @test (ncheckpoints[i] - ncheckpoints_average) < .1*ncheckpoints_average
    end

    rmprocs(workers())
    @test x ≈ (sum(a*b*[1:100;])) * ones(10)
    rm.(tmpdirs; recursive=true, force=true)
end

@testset "epmapreduce, timeout during f eval, and skip_tasks_that_timeout=true" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere function foo17(x, tsk, a; b)
        if tsk == 20
            sleep(600)
        else
            sleep(0.1)
        end
        x .+= a*b*tsk
        nothing
    end

    tmpdirs = [mktempdir(;cleanup=false) for i=1:3]

    a,b = 2,3
    options = SchedulerOptions(;scratch=tmpdirs, keepcheckpoints=true, skip_tasks_that_timeout=true)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo17, 1:100, a; b=b)
    @test tsks == [20]

    ncheckpoints = [length(readdir(tmpdir)) for tmpdir in tmpdirs]
    ncheckpoints_average = sum(ncheckpoints) / 3
    for i = 1:3
        @test ncheckpoints[i] > 0
        @test (ncheckpoints[i] - ncheckpoints_average) < .1*ncheckpoints_average
    end

    rmprocs(workers())
    @test x ≈ (sum(a*b*[1:100;]) - a*b*20) * ones(10)
    rm.(tmpdirs; recursive=true, force=true)
end

@testset "pmapreduce with task checkpoint and restart" begin
    s = Dict{Int,Future}()
    p = Dict{Int,Future}()

    function init(pid, s)
        s[pid] = remotecall(ones, pid, Int, 1)
        p[pid] = remotecall(Channel{Bool}, pid, 1)
    end

    r = randstring('a':'z', 6)

    function foo18(result, tsk, s, r)
        _s = fetch(s[myid()])::Vector{Int}
        for i = _s[1]:10
            _s .= i
            touch("testfile-$r-$tsk-$i.txt")
            @info "_s on pid=$(myid()) is $(_s[1])"
            sleep(5)
        end
        result .+= _s[1]
        _s .= 1
    end

    function checkpoint_task(tsk, s)
        @info "checkpoint task..."
        _s = fetch(s[myid()])::Vector{Int}
        write("task_checkpoint_$tsk.bin", _s)
        @info "...checkpoint task with state=$(_s[1])."
    end

    function restart_task!(tsk, s)
        @info "restart task..."
        _s = fetch(s[myid()])::Vector{Int}
        if isfile("task_checkpoint_$tsk.bin")
            read!("task_checkpoint_$tsk.bin", _s)
            rm("task_checkpoint_$tsk.bin")
        end
        @info "...restart task, " _s
    end

    function signal_preempt(p)
        put!(fetch(p[myid()]), true)
    end

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(;
        minworkers = 0,
        maxworkers = 2,
        init = pid->init(pid, s),
        checkpoint_task = tsk->checkpoint_task(tsk, s),
        restart_task = tsk->restart_task!(tsk, s),
        preempt_channel_future = pid->p[pid],
        scratch = tmpdir
    )

    t = @async epmapreduce!(zeros(2), options, (result,i)->foo18(result,i,s,r), 1:4)

    sleep(25)
    while nprocs() == 1
        sleep(25)
    end
    remotecall_wait(signal_preempt, workers()[1], p)

    result,tsks = fetch(t)
    @test result ≈ [40.0,40.0]
    files = filter(f->startswith(f, "testfile-$r"), readdir())
    @test length(files) == 40
    rm.(files)
    rm(tmpdir; recursive=true, force=true)
end

@testset "streaming reduction, chunks" begin
    layout = Schedulers.checkpoint_layout((; a=zeros(Float32, 10), b=zeros(Float64, 7), c=zeros(Int8, 3)))
    @test layout == [(Float32, 40), (Float64, 56), (Int8, 3)]
    for chunkbytes in (1, 3, 8, 20, 1000)
        chunks = Schedulers.checkpoint_chunks(layout, chunkbytes)
        nbytes = [chunk.nbytes for chunk in chunks]
        @test sum(nbytes) == 99
        @test [chunk.offset for chunk in chunks] == cumsum([0; nbytes[1:end-1]])
        for chunk in chunks
            @test chunk.T == layout[chunk.array][1]
            @test chunk.nbytes % sizeof(chunk.T) == 0
            @test chunk.arrayoffset + chunk.nbytes <= layout[chunk.array][2]
        end
    end
    @test_throws ErrorException Schedulers.checkpoint_arrays(Dict(1=>2))
    @test_throws ErrorException Schedulers.checkpoint_arrays([[1.0]])
end

@testset "streaming reduction, local files" begin
    tmpdir = mktempdir()
    zeros_xyz() = (; x=zeros(Float32, 100), y=zeros(Float64, 3, 5), z=zeros(Int32, 7))
    T = typeof(zeros_xyz())
    layout = Schedulers.checkpoint_layout(zeros_xyz())

    inputs = [joinpath(tmpdir, "input-$i") for i in 1:3]
    for (i, input) in enumerate(inputs)
        x = zeros_xyz()
        foreach(a->a .= i, Schedulers.checkpoint_arrays(x))
        Schedulers.save_checkpoint_streaming(identity, input, x, T, 24, 2)
        @test filesize(input) == sum(last, layout)
    end

    output = joinpath(tmpdir, "output")
    Schedulers.reduce_streaming(Schedulers.default_reducer_chunk!, inputs, output, layout, 24, 2)
    @test !isfile(Schedulers.partial_checkpoint(output))
    r = Schedulers.load_checkpoint_streaming(zeros_xyz, output, T, 24, 2)
    @test all(a->all(==(6), a), Schedulers.checkpoint_arrays(r))

    # a retried reduction into memory skips the chunks that are already reduced
    s = zeros_xyz()
    done = fill(false, length(Schedulers.checkpoint_chunks(layout, 24)))
    done[1] = true
    Schedulers.reduce_into!(Schedulers.default_reducer_chunk!, s, [output], 24, 2; done)
    @test all(done)
    @test all(==(0), s.x[1:6]) && all(==(6), s.x[7:end]) && all(==(6), s.y) && all(==(6), s.z)

    # inputs that are not streaming checkpoints of the expected size are rejected, and nothing is written
    bad, output2 = joinpath(tmpdir, "bad"), joinpath(tmpdir, "output2")
    write(bad, rand(UInt8, 10))
    @test_throws ErrorException Schedulers.reduce_streaming(Schedulers.default_reducer_chunk!, [bad, inputs[1]], output2, layout, 24, 2)
    @test !isfile(output2) && !isfile(Schedulers.partial_checkpoint(output2))

    rm(tmpdir; recursive=true)
end

# a streaming checkpoint in memory, with hooks that run before each chunk is read or written
mutable struct MemoryCheckpoint
    data::Vector{UInt8}
    onread::Function
    onwrite::Function
    committed::Bool
    aborted::Bool
    written::Vector{Int} # for each chunk, the copy that wrote it last (chunks are at least a byte, so there are no more chunks than bytes)
    kept::Vector{Int}    # for each chunk, the copy that the commit kept
end
MemoryCheckpoint(data; onread=offset->nothing, onwrite=offset->nothing) = MemoryCheckpoint(data, onread, onwrite, false, false, zeros(Int, length(data)), Int[])
Base.filesize(c::MemoryCheckpoint) = length(c.data)
Schedulers.checkpoint_read!(c::MemoryCheckpoint, buffer, offset) = (c.onread(offset); copyto!(buffer, 1, c.data, offset + 1, length(buffer)); nothing)
Schedulers.checkpoint_write_chunk(c::MemoryCheckpoint, ichunk, nchunks, buffer, offset, copy) = (c.onwrite(offset); copyto!(c.data, offset + 1, buffer, 1, length(buffer)); c.written[ichunk] = copy; nothing)
Schedulers.checkpoint_commit(c::MemoryCheckpoint, copies) = (c.committed = true; c.kept = collect(copies); nothing)
Schedulers.checkpoint_abort(c::MemoryCheckpoint) = (c.aborted = true; nothing)

@testset "streaming reduction, later chunks are read while earlier chunks are written" begin
    depth, chunkbytes, nchunks = 2, 8, 8
    # every write waits until a chunk past the first `depth` is read, which only happens if reading goes on while chunks are written
    later_chunk_read = Threads.Atomic{Bool}(false)
    inputs = [MemoryCheckpoint(fill(UInt8(i), nchunks*chunkbytes); onread=offset->(offset >= depth*chunkbytes && (later_chunk_read[] = true))) for i in 1:3]
    output = MemoryCheckpoint(zeros(UInt8, nchunks*chunkbytes); onwrite=offset->(timedwait(()->later_chunk_read[], 30) === :ok || error("no chunk was read while chunks were written")))
    Schedulers.reduce_streaming(Schedulers.default_reducer_chunk!, inputs, output, [(UInt8, nchunks*chunkbytes)], chunkbytes, depth)
    @test later_chunk_read[]
    @test output.committed && !output.aborted
    @test all(==(UInt8(6)), output.data)
end

@testset "streaming reduction, a failed $stage aborts the output" for stage in ("read", "write")
    depth, chunkbytes, nchunks = 2, 8, 8
    inject(offset) = offset == 4*chunkbytes && error("injected $stage failure")
    inputs = [MemoryCheckpoint(fill(UInt8(i), nchunks*chunkbytes); onread = stage == "read" ? inject : offset->nothing) for i in 1:3]
    output = MemoryCheckpoint(zeros(UInt8, nchunks*chunkbytes); onwrite = stage == "write" ? inject : offset->nothing)
    t = Threads.@spawn Schedulers.reduce_streaming(Schedulers.default_reducer_chunk!, inputs, output, [(UInt8, nchunks*chunkbytes)], chunkbytes, depth)
    @test timedwait(()->istaskdone(t), 30) === :ok # no deadlock
    @test occursin("injected $stage failure", try fetch(t); "" catch e; sprint(showerror, e) end)
    @test output.aborted && !output.committed
end

@testset "pmapreduce, streaming reduction, reduce_fanin=$fanin" for fanin in (2, 3, 4)
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers
    @everywhere function foo_streaming(r, tsk)
        r.x .+= tsk
        r.y .+= 2tsk
        r.z .+= 3tsk
        nothing
    end

    tmpdir = mktempdir(;cleanup=false)
    zeros_xyz() = (; x=zeros(Float32, 100), y=zeros(Float64, 3, 5), z=zeros(Int32, 7))
    options = SchedulerOptions(;scratch=tmpdir, zeros=zeros_xyz, reduce_streaming=true, reduce_fanin=fanin, reduce_chunkbytes=100, reduce_depth=3)
    r,tsks = epmapreduce!(zeros_xyz(), options, foo_streaming, 1:40)
    rmprocs(workers())

    s = sum(1:40)
    @test all(==(s), r.x)
    @test all(==(2s), r.y)
    @test all(==(3s), r.z)
    @test isempty(readdir(tmpdir)) # no committed or partial checkpoints are left behind
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, partial reduction, streaming" begin
    using Distributed, Schedulers

    function foo16s(r, i)
        r .+= i
        sleep(2)
        nothing
    end

    function my_reduce_trigger_s(eloop, ntasks)
        if length(complete_tasks(eloop)) - ntasks[] > 30
            trigger_reduction!(eloop)
            ntasks[] = length(complete_tasks(eloop))
        end
    end

    tmpdir = mktempdir(;cleanup=false)
    tmpfile = tempname()

    ntasks = Ref(0)
    options = SchedulerOptions(;maxworkers=10, scratch=tmpdir, reduce_trigger=eloop->my_reduce_trigger_s(eloop, ntasks), save_partial_reduction=input->write(tmpfile, input),
        reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=16)
    r,tsks = epmapreduce!(zeros(10), options, foo16s, 1:60)

    @test r ≈ sum([1:60;]) * ones(10)

    x = read!(tmpfile, zeros(10))
    @test x[1] >= sum([1:30;])
    for i in eachindex(x)
        @test x[i] ≈ x[1]
    end

    rm(tmpfile)
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, partial reduction, streaming, snapshot smaller than reduce_fanin" begin
    using Distributed, Schedulers

    function foo_small_snapshot(r, i)
        r .+= i
        sleep(0.5)
        nothing
    end

    function trigger_small_snapshot(eloop, ntasks)
        if length(complete_tasks(eloop)) - ntasks[] >= 4
            trigger_reduction!(eloop)
            ntasks[] = length(complete_tasks(eloop))
        end
    end

    # with 2 workers, the first snapshot holds 2 checkpoints, fewer than reduce_fanin, and no reducer is running yet
    tmpdir = mktempdir(;cleanup=false)
    tmpfile = tempname()

    ntasks = Ref(0)
    options = SchedulerOptions(;maxworkers=2, scratch=tmpdir, reduce_trigger=eloop->trigger_small_snapshot(eloop, ntasks), save_partial_reduction=input->write(tmpfile, input),
        reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=16)
    t = @async epmapreduce!(zeros(10), options, foo_small_snapshot, 1:20)
    @test timedwait(()->istaskdone(t), 300.0; pollint=1.0) == :ok # it used to hang

    if istaskdone(t)
        r,tsks = fetch(t)
        @test r ≈ sum([1:20;]) * ones(10)
        @test isfile(tmpfile)
        rm(tmpfile; force=true)
    end
    rm(tmpdir; recursive=true, force=true)
end

@testset "streaming reduction, a custom reducer! needs a reducer_chunk!" begin
    myreducer!(x, y) = (x .+= y; nothing)
    mychunk!(x, y) = (x .+= y; nothing)
    check(; kwargs...) = Schedulers.check_streaming_reducer(SchedulerOptions(; kwargs...))
    @test_throws ArgumentError check(; reduce_streaming=true, reducer! = myreducer!)
    @test check(; reduce_streaming=true, reducer! = myreducer!, reducer_chunk! = mychunk!) === nothing
    @test check(; reduce_streaming=true, reducer! = myreducer!, reducer_chunk! = Schedulers.default_reducer_chunk!) === nothing
    @test check(; reduce_streaming=true) === nothing
    @test check(; reduce_streaming=false, reducer! = myreducer!) === nothing

    # epmapreduce! fails before running anything, and leaves the options as they were
    tmpdir = mktempdir()
    options = SchedulerOptions(; scratch=tmpdir, reduce_streaming=true, reducer! = myreducer!)
    e = try
        epmapreduce!(zeros(10), options, (r, tsk)->nothing, 1:4)
        nothing
    catch e
        e
    end
    @test e isa ArgumentError && occursin("reducer_chunk!", e.msg)
    @test options.reducer_chunk! === nothing
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, streaming reduction, custom reducer_chunk!" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers
    @everywhere begin
        foo_summax(r, tsk) = (r.s .+= tsk; r.m .= max.(r.m, tsk); nothing)
        # the summed array holds Float32 and the maximum Int32, so reducer_chunk! can tell their chunks apart
        chunk_summax!(x::AbstractVector{Float32}, y) = (x .+= y; nothing)
        chunk_summax!(x::AbstractVector{Int32}, y) = (x .= max.(x, y); nothing)
    end

    tmpdir = mktempdir(;cleanup=false)
    zeros_summax() = (; s=zeros(Float32, 100), m=zeros(Int32, 100))
    options = SchedulerOptions(;scratch=tmpdir, zeros=zeros_summax, reducer! = (x, y)->(x.s .+= y.s; x.m .= max.(x.m, y.m); nothing),
        reducer_chunk! = chunk_summax!, reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=64)
    r,tsks = epmapreduce!(zeros_summax(), options, foo_summax, 1:30)
    rmprocs(workers())

    @test all(==(sum(1:30)), r.s)
    @test all(==(30), r.m)
    rm(tmpdir; recursive=true, force=true)
end

@testset "remote calls return as soon as they finish" begin
    safe_addprocs(1)
    @everywhere using Distributed, Schedulers
    pid = workers()[1]
    call_wait(f, times=Float64[]) = Schedulers.remotecall_wait_timeout(times, 1, 5.0, nothing, tsk->nothing, tsk->nothing, 0, f, pid)
    call_fetch() = Schedulers.remotecall_fetch_timeout(60, 1, 1, nothing, tsk->nothing, tsk->nothing, 0, gethostname, pid)
    call_wait(()->nothing); call_fetch() # compile
    # the fastest of a few calls, so that an occasional pause (e.g. garbage collection) does not count; waiting with
    # sleep(1) made every call take at least a second
    fastest(f) = minimum(_->@elapsed(f()), 1:3)

    @test fastest(()->call_wait(()->nothing)) < 0.5
    @test fastest(call_fetch) < 0.5
    # failures are reported right away too
    @test fastest(()->try call_wait(()->error("expected error")) catch end) < 0.5
    # and timeouts still apply: the limit here is 5 times the median of the earlier times, 1 second
    t = @elapsed @test_throws Schedulers.TimeoutException call_wait(()->sleep(5), [0.2, 0.2])
    @test 1 <= t < 3
    # fast calls are timed as taking a second, and others are timed in whole seconds, rounded up, as when calls were
    # polled once a second, so that timeouts based on them stay as they were
    times = Float64[]
    call_wait(()->nothing, times)
    call_wait(()->sleep(1.2), times)
    @test times == [1.0, 2.0]
    rmprocs(workers())
end

@testset "streaming reduction, group sizes" begin
    # a full group, or the remaining checkpoints once no more can arrive
    @test Schedulers.reduce_group_size(7, 5, true) == 5
    @test Schedulers.reduce_group_size(5, 5, false) == 5
    @test Schedulers.reduce_group_size(4, 5, true) == 0
    @test Schedulers.reduce_group_size(4, 5, false) == 4
    @test Schedulers.reduce_group_size(1, 5, false) == 0
    # with a fan-in of 2, any two checkpoints are a full group, as before
    @test Schedulers.reduce_group_size(2, 2, true) == 2
end

@testset "pmapreduce, streaming reduction, merges take full groups" begin
    safe_addprocs(10)
    @everywhere using Distributed, Schedulers
    # tasks that take a while, so that workers that start late get tasks too, and there are checkpoints to merge
    @everywhere foo_groups(r, tsk) = (r .+= tsk; sleep(0.5); nothing)

    tmpdir = mktempdir(;cleanup=false)
    fanin = 3
    options = SchedulerOptions(;scratch=tmpdir, reduce_streaming=true, reduce_fanin=fanin, reduce_chunkbytes=64)
    # catch_exceptions=true, as with the default loggers, so that a failure to build a debug message can not change the run
    logger = Test.TestLogger(; min_level=Logging.Debug, catch_exceptions=true)
    r,tsks = with_logger(logger) do
        epmapreduce!(zeros(Float32, 100), options, foo_groups, 1:40)
    end
    rmprocs(workers())

    ninputs = [parse(Int, m.captures[1]) for m in (match(r"^reducing into checkpoint3, pid=\d+, from (\d+) checkpoints", string(l.message)) for l in logger.logs) if m !== nothing]
    ncheckpoints = 1 + sum(k->k - 1, ninputs; init=0)
    @test all(==(sum(1:40)), r)
    @test !isempty(ninputs)
    @test length(ninputs) == cld(ncheckpoints - 1, fanin - 1) # as few merges as possible
    @test all(==(fanin), ninputs[1:end-1])                    # only the last merge may take fewer checkpoints
    rm(tmpdir; recursive=true, force=true)
end

@testset "streaming reduction, parts of a merge" begin
    # parts are contiguous ranges of chunks, that together hold all of the chunks, none empty
    for layout in ([(Float32, 400)], [(Int8, 3), (Float64, 800), (Float32, 4)]), chunkbytes in (8, 64)
        chunks = Schedulers.checkpoint_chunks(layout, chunkbytes)
        for nparts in 1:length(chunks)
            ranges = Schedulers.part_ranges(chunks, nparts)
            @test vcat(collect.(ranges)...) == collect(eachindex(chunks))
            @test all(!isempty, ranges)
            @test all(part->Schedulers.part_chunks(chunks, part, nparts) == ranges[part], 1:nparts)
        end
    end
    # of about the same size
    chunks = Schedulers.checkpoint_chunks([(Float32, 4000)], 64)
    sizes = [sum(ichunk->chunks[ichunk].nbytes, ichunks) for ichunks in Schedulers.part_ranges(chunks, 7)]
    @test maximum(sizes) - minimum(sizes) <= 64
    @test_throws ArgumentError Schedulers.part_ranges(chunks, length(chunks) + 1)
    @test_throws ArgumentError Schedulers.part_ranges(chunks, 0)

    # the number of parts is bounded by the part size, and by the number of chunks
    chunks = Schedulers.checkpoint_chunks([(Float32, 400)], 64)
    @test Schedulers.max_merge_parts(chunks, typemax(Int)) == 1
    @test Schedulers.max_merge_parts(chunks, 1000) == 1
    @test Schedulers.max_merge_parts(chunks, 128) == 3
    @test Schedulers.max_merge_parts(chunks, 1) == length(chunks)

    # parts, run at once and in any order, write all of the merged checkpoint, and leave committing it to the caller
    depth, chunkbytes, nchunks, nparts = 2, 8, 9, 4
    layout = [(UInt8, nchunks*chunkbytes)]
    inputs = [MemoryCheckpoint(fill(UInt8(i), nchunks*chunkbytes)) for i in 1:3]
    output = MemoryCheckpoint(zeros(UInt8, nchunks*chunkbytes))
    @sync for part in nparts:-1:1
        Threads.@spawn Schedulers.reduce_streaming_part(Schedulers.default_reducer_chunk!, inputs, output, layout, chunkbytes, depth, part, nparts)
    end
    @test all(==(UInt8(6)), output.data)
    @test !output.committed && !output.aborted

    # a failed part throws, and leaves aborting the output to the caller too, since other parts may still write to it
    inputs[2].onread = offset->offset >= 4*chunkbytes && error("injected read failure")
    output = MemoryCheckpoint(zeros(UInt8, nchunks*chunkbytes))
    @test_throws Exception Schedulers.reduce_streaming_part(Schedulers.default_reducer_chunk!, inputs, output, layout, chunkbytes, depth, nparts, nparts)
    @test !output.committed && !output.aborted

    # local files: the merged checkpoint appears once it is committed
    tmpdir = mktempdir()
    zeros_xyz() = (; x=zeros(Float32, 100), y=zeros(Float64, 3, 5), z=zeros(Int32, 7))
    T = typeof(zeros_xyz())
    layout = Schedulers.checkpoint_layout(zeros_xyz())
    files = [joinpath(tmpdir, "input-$i") for i in 1:3]
    for (i, file) in enumerate(files)
        x = zeros_xyz()
        foreach(a->a .= i, Schedulers.checkpoint_arrays(x))
        Schedulers.save_checkpoint_streaming(identity, file, x, T, 24, 2)
    end
    merged = joinpath(tmpdir, "merged")
    for part in 3:-1:1
        Schedulers.reduce_streaming_part(Schedulers.default_reducer_chunk!, files, merged, layout, 24, 2, part, 3)
    end
    @test !isfile(merged)
    Schedulers.checkpoint_commit(merged, fill(1, length(Schedulers.checkpoint_chunks(layout, 24))))
    r = Schedulers.load_checkpoint_streaming(zeros_xyz, merged, T, 24, 2)
    @test all(a->all(==(6), a), Schedulers.checkpoint_arrays(r))
    rm(tmpdir; recursive=true)
end

@testset "streaming reduction, timeouts of parts of a merge" begin
    # a part of a merge times out like a whole merge, however small its share of the merge
    times = [300.0; fill(30.0, 20)]
    @test Schedulers.maximum_task_time(Schedulers.PartTimes(times), 10, 5) == Schedulers.maximum_task_time(times, 10, 5) == 150.0
    # and its time is not recorded with those of whole merges
    push!(Schedulers.PartTimes(times), 3.0)
    @test length(times) == 21
end

@testset "streaming reduction, number of parts of a merge" begin
    safe_addprocs(3)
    # earlier tests can leave workers behind, so only these three count as initialized
    pids = workers()[1:3]
    eloop = Schedulers.ElasticLoop(String, 1:0, SchedulerOptions(; reduce_streaming=true, reduce_fanin=3); isreduce=true)
    # the workers are up and idle, and 1000 and 1001 were removed since they were initialized
    union!(eloop.initialized_pids, pids, [1000, 1001])
    @test Schedulers.merge_parts(eloop, 10, 0) == 3 # one part for this merge's reducer, and one for each other idle worker
    @test Schedulers.merge_parts(eloop, 2, 0) == 2  # at most maxparts
    @test Schedulers.merge_parts(eloop, 1, 0) == 1
    @test Schedulers.merge_parts(eloop, 10, 1) == 2 # a worker is left for another group of checkpoints that waits
    eloop.reduce_busy = 1
    @test Schedulers.merge_parts(eloop, 10, 0) == 2 # a worker is busy reducing
    eloop.reduce_busy = 0
    push!(eloop.reduce_parts, Schedulers.MergeCopy(Schedulers.Merge(["a", "b"], "c"), 2))
    @test Schedulers.merge_parts(eloop, 10, 0) == 2 # a part of another merge waits for a worker
    empty!(eloop.reduce_parts)
    push!(eloop.used_pids_map, pids[1])
    @test Schedulers.merge_parts(eloop, 10, 0) == 2 # a worker maps
    push!(eloop.removed_pids, pids[2])
    @test Schedulers.merge_parts(eloop, 10, 0) == 1 # a worker is being removed
    push!(eloop.tsk_pool_todo, 1)
    @test Schedulers.merge_parts(eloop, 10, 0) == 1 # map tasks come first
    # maxreduceworkers bounds the workers that reduce at once
    eloop = Schedulers.ElasticLoop(String, 1:0, SchedulerOptions(; reduce_streaming=true, maxreduceworkers=2); isreduce=true)
    union!(eloop.initialized_pids, pids)
    @test Schedulers.merge_parts(eloop, 10, 0) == 2
    rmprocs(workers())
end

@testset "pmapreduce, streaming reduction, merges split into parts" begin
    safe_addprocs(8)
    @everywhere using Distributed, Schedulers
    # tasks that take a while, so that every worker runs one, and there are checkpoints to merge
    @everywhere foo_parts(r, tsk) = (r .+= tsk; sleep(2); nothing)

    tmpdir = mktempdir(;cleanup=false)
    function run_parts(; kwargs...)
        options = SchedulerOptions(;scratch=tmpdir, reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=64, reduce_partbytes=128, kwargs...)
        logger = Test.TestLogger(; min_level=Logging.Debug, catch_exceptions=true)
        r,tsks = with_logger(logger) do
            epmapreduce!(zeros(Float32, 100), options, foo_parts, 1:8)
        end
        r, [string(l.message) for l in logger.logs]
    end

    # with as many workers as tasks, workers are idle when merges start, so merges are split
    r, msgs = run_parts()
    nsplit = count(m->startswith(m, "splitting the merge"), msgs)
    pids = unique(parse(Int, match(r"pid=(\d+)", m).captures[1]) for m in msgs if startswith(m, "reducing part"))
    @test all(==(sum(1:8)), r)
    @test count(m->startswith(m, "reducing from"), msgs) > 0                    # there were merges
    @test nsplit > 0
    @test count(m->startswith(m, "pushed merged checkpoint3 from"), msgs) == nsplit # every split merge was committed
    @test length(pids) > 1                                                         # by several workers
    @test isempty(readdir(tmpdir))

    # merges are not split beyond maxreduceworkers
    r, msgs = run_parts(; maxreduceworkers=1)
    @test all(==(sum(1:8)), r)
    @test !any(m->startswith(m, "splitting the merge"), msgs)
    @test isempty(readdir(tmpdir))

    rmprocs(workers())
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, streaming reduction, a failed part of a merge" begin
    safe_addprocs(6)
    @everywhere using Distributed, Schedulers
    failfile = tempname()
    touch(failfile)
    @everywhere failfile = $failfile
    @everywhere begin
        foo_failed_part(r, tsk) = (r .+= tsk; sleep(2); nothing)
        # the first chunk to be reduced fails, on whichever worker reduces it
        function flaky_chunk!(x, y)
            isfile(failfile) && (try rm(failfile); true catch; false end) && error("injected failure")
            x .+= y
            nothing
        end
    end

    tmpdir = mktempdir(;cleanup=false)
    options = SchedulerOptions(;scratch=tmpdir, reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=64, reduce_partbytes=128, reducer_chunk! = flaky_chunk!)
    logger = Test.TestLogger(; min_level=Logging.Debug, catch_exceptions=true)
    r,tsks = with_logger(logger) do
        epmapreduce!(zeros(Float32, 100), options, foo_failed_part, 1:6)
    end
    rmprocs(workers())
    msgs = [string(l.message) for l in logger.logs]

    @test !isfile(failfile)                                                    # the failure was injected
    @test any(m->occursin("caught exception during part", m), msgs)            # into a part of a split merge
    @test all(==(sum(1:6)), r)                                                 # which was done again
    @test isempty(readdir(tmpdir))                                             # and its partial output removed
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, partial reduction, streaming, merges split into parts" begin
    using Distributed, Schedulers

    function foo_split_trigger(r, i)
        r .+= i
        sleep(2)
        nothing
    end

    function trigger_split(eloop, ntasks)
        if length(complete_tasks(eloop)) - ntasks[] >= 10
            trigger_reduction!(eloop)
            ntasks[] = length(complete_tasks(eloop))
        end
    end

    # by the first trigger, all tasks have started, so workers are idle, and the merges of the snapshot can be split
    tmpdir = mktempdir(;cleanup=false)
    tmpfile = tempname()

    ntasks = Ref(0)
    options = SchedulerOptions(;maxworkers=10, scratch=tmpdir, reduce_trigger=eloop->trigger_split(eloop, ntasks), save_partial_reduction=input->write(tmpfile, input),
        reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=16, reduce_partbytes=16)
    t = @async epmapreduce!(zeros(10), options, foo_split_trigger, 1:20)
    @test timedwait(()->istaskdone(t), 300.0; pollint=1.0) == :ok

    if istaskdone(t)
        r,tsks = fetch(t)
        @test r ≈ sum([1:20;]) * ones(10)
        x = read!(tmpfile, zeros(10))
        @test x[1] >= sum([1:10;])
        @test all(≈(x[1]), x)
        rm(tmpfile; force=true)
    end
    rm(tmpdir; recursive=true, force=true)
end

@testset "streaming reduction, copies of the chunks of a merge" begin
    # block ids: of the same length for every chunk and copy, and valid base64 strings (letters and digits, a multiple of 4 long)
    ids = [Schedulers.chunk_blockid(ichunk, copy) for ichunk in (1, 2, 50_000), copy in (1, 2, 999)]
    @test allunique(ids)
    @test all(id->occursin(r"^[A-Za-z0-9]{16}$", id), ids)
    @test_throws ArgumentError Schedulers.chunk_blockid(0, 1)
    @test_throws ArgumentError Schedulers.chunk_blockid(1, 0)

    # a copy writes its chunks as that copy, and the commit keeps the copies it is told to
    depth, chunkbytes, nchunks, nparts = 2, 8, 9, 3
    layout = [(UInt8, nchunks*chunkbytes)]
    chunks = Schedulers.checkpoint_chunks(layout, chunkbytes)
    inputs = [MemoryCheckpoint(fill(UInt8(i), nchunks*chunkbytes)) for i in 1:3]
    output = MemoryCheckpoint(zeros(UInt8, nchunks*chunkbytes))
    @test Schedulers.reduce_streaming_part(Schedulers.default_reducer_chunk!, inputs, output, layout, chunkbytes, depth, 2, nparts; copy=7) == :done
    ichunks = Schedulers.part_chunks(chunks, 2, nparts)
    @test all(==(7), output.written[ichunks])
    @test all(==(0), output.written[setdiff(1:nchunks, ichunks)])

    # a copy stops once it is cancelled, here while it reads its first chunk
    id = Schedulers.next_copy_id()
    inputs[1].onread = offset->Schedulers.cancel_copy(id)
    output = MemoryCheckpoint(zeros(UInt8, nchunks*chunkbytes))
    @test Schedulers.reduce_streaming_part(Schedulers.default_reducer_chunk!, inputs, output, layout, chunkbytes, depth, 1, 1; copy=2, id) == :cancelled
    @test count(!=(0), output.written) < nchunks
    @test !haskey(Schedulers.COPIES, id) # the copy is forgotten once it returns

    # a cancel can reach the worker before its copy starts: the copy then starts cancelled, and writes nothing
    id = Schedulers.next_copy_id()
    Schedulers.cancel_copy(id)
    inputs[1].onread = offset->nothing
    output = MemoryCheckpoint(zeros(UInt8, nchunks*chunkbytes))
    @test Schedulers.reduce_streaming_part(Schedulers.default_reducer_chunk!, inputs, output, layout, chunkbytes, depth, 1, 1; copy=3, id) == :cancelled
    @test all(==(0), output.written)
    @test !haskey(Schedulers.COPIES, id) && !haskey(Schedulers.CANCELLED_BEFORE_START, id) # and the cancel is used up
    # cancels of copies that never start, or that already stopped, are forgotten after an hour
    Schedulers.CANCELLED_BEFORE_START[id] = time() - 7200
    late = Schedulers.next_copy_id()
    Schedulers.cancel_copy(late)
    @test !haskey(Schedulers.CANCELLED_BEFORE_START, id) && haskey(Schedulers.CANCELLED_BEFORE_START, late)
    delete!(Schedulers.CANCELLED_BEFORE_START, late)

    # local files: copies of a part write the same bytes to the same partial file, and chunks that a copy that lost writes
    # after the commit are discarded
    tmpdir = mktempdir()
    zeros_xyz() = (; x=zeros(Float32, 100), y=zeros(Float64, 3, 5), z=zeros(Int32, 7))
    T = typeof(zeros_xyz())
    layout = Schedulers.checkpoint_layout(zeros_xyz())
    files = [joinpath(tmpdir, "input-$i") for i in 1:3]
    for (i, file) in enumerate(files)
        x = zeros_xyz()
        foreach(a->a .= i, Schedulers.checkpoint_arrays(x))
        Schedulers.save_checkpoint_streaming(identity, file, x, T, 24, 2)
    end
    merged = joinpath(tmpdir, "merged")
    @sync for part in 1:3, copy in 1:2
        Threads.@spawn Schedulers.reduce_streaming_part(Schedulers.default_reducer_chunk!, files, merged, layout, 24, 2, part, 3; copy=2part + copy)
    end
    nchunks = length(Schedulers.checkpoint_chunks(layout, 24))
    Schedulers.checkpoint_commit(merged, fill(3, nchunks))
    r = Schedulers.load_checkpoint_streaming(zeros_xyz, merged, T, 24, 2)
    @test all(a->all(==(6), a), Schedulers.checkpoint_arrays(r))
    Schedulers.reduce_streaming_part(Schedulers.default_reducer_chunk!, files, merged, layout, 24, 2, 1, 3; copy=9)
    @test isfile(Schedulers.partial_checkpoint(merged))
    Schedulers.checkpoint_abort(merged)
    @test !isfile(Schedulers.partial_checkpoint(merged)) && isfile(merged)
    rm(tmpdir; recursive=true)
end

@testset "streaming reduction, a cancel that reaches the worker before its copy" begin
    safe_addprocs(1)
    @everywhere using Distributed, Schedulers
    pid = workers()[1]
    tmpdir = mktempdir()
    layout = [(Float32, 2^20)]
    files = [joinpath(tmpdir, "input-$i") for i in 1:3]
    for (i, file) in enumerate(files)
        Schedulers.save_checkpoint_streaming(identity, file, fill(Float32(i), 2^18), Vector{Float32}, 2^16, 2)
    end
    # the master sends the cancel first, as when another copy of the part finishes while this one is being started
    for copy in 1:5
        id = Schedulers.next_copy_id()
        remote_do(Schedulers.cancel_copy, pid, id)
        output = joinpath(tmpdir, "output-$copy")
        @test remotecall_fetch(Schedulers.reduce_streaming_part, pid, Schedulers.default_reducer_chunk!, files, output, layout, 2^16, 2, 1, 1; copy, id) == :cancelled
        @test !isfile(Schedulers.partial_checkpoint(output)) # it wrote nothing
    end
    rmprocs(workers())
    rm(tmpdir; recursive=true)
end

@testset "streaming reduction, spare work" begin
    eloop = Schedulers.ElasticLoop(String, 1:1, SchedulerOptions(; reduce_streaming=true, reduce_resplit=true); isreduce=true)
    eloop.reduce_maxparts = 4
    merge = Schedulers.Merge(["a", "b"], "c")
    push!(eloop.reduce_whole, merge)
    push!(eloop.reduce_spares, Schedulers.MergeCopy(Schedulers.Merge(["d", "e"], "f"), 1))
    # spare work waits for the map to be done
    @test !Schedulers.spare_work_allowed(eloop)
    @test Schedulers.reduce_spare_work(eloop) == 0
    empty!(eloop.tsk_pool_todo)
    @test Schedulers.spare_work_allowed(eloop)
    # a merge that runs whole is worth splitting while what is left of it takes more than twice as long as a part
    @test Schedulers.resplit_worthwhile(eloop, merge) # until a whole merge finished, it is
    push!(eloop.reduce_times_whole, 30.0)
    push!(eloop.reduce_times_part, 3.0)
    merge.tic = time() - 10
    @test Schedulers.resplit_worthwhile(eloop, merge)
    @test Schedulers.reduce_spare_work(eloop) == 1 + 4 # the queued extra copy, and up to `reduce_maxparts` parts of the merge
    merge.tic = time() - 25
    @test !Schedulers.resplit_worthwhile(eloop, merge)
    @test Schedulers.reduce_spare_work(eloop) == 1
    eloop.reduce_resplit = false
    merge.tic = time() - 10
    @test Schedulers.reduce_spare_work(eloop) == 1
end

@testset "pmapreduce, streaming reduction, copies of parts" begin
    safe_addprocs(8)
    @everywhere using Distributed, Schedulers
    slowpid = workers()[1]
    @everywhere slowpid = $slowpid
    @everywhere begin
        foo_copies(r, tsk) = (r .+= tsk; sleep(2); nothing)
        # the copies that run on `slowpid` are slow
        chunk_slow!(x, y) = (myid() == slowpid && sleep(0.5); x .+= y; nothing)
    end

    tmpdir = mktempdir(;cleanup=false)
    options = SchedulerOptions(;scratch=tmpdir, reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=64, reduce_partbytes=128, reduce_partcopies=2, reducer_chunk! = chunk_slow!)
    logger = Test.TestLogger(; min_level=Logging.Debug, catch_exceptions=true)
    r,tsks = with_logger(logger) do
        epmapreduce!(zeros(Float32, 200), options, foo_copies, 1:8)
    end
    rmprocs(workers())
    msgs = [string(l.message) for l in logger.logs]

    nstarted = count(m->startswith(m, "splitting the merge") || startswith(m, "reducing into checkpoint3"), msgs)
    ncommitted = count(m->startswith(m, "pushed merged checkpoint3") || startswith(m, "pushed reduced checkpoint3"), msgs)
    @test all(==(sum(1:8)), r)
    @test nstarted == ncommitted                                       # every merge that started was committed: none was done again
    @test any(m->occursin(r"^copy \d+ of part \d+ of \d+ into checkpoint3 lost", m), msgs) # extra copies ran, and lost
    @test !any(m->occursin("caught exception", m), msgs)
    @test isempty(readdir(tmpdir))                                     # copies that lost left nothing behind
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, streaming reduction, a failed copy of a part" begin
    safe_addprocs(6)
    @everywhere using Distributed, Schedulers
    failfile = tempname()
    touch(failfile)
    @everywhere failfile = $failfile
    @everywhere begin
        foo_failed_copy(r, tsk) = (r .+= tsk; sleep(2); nothing)
        # the first chunk to be reduced fails, on whichever worker reduces it
        function flaky_copy_chunk!(x, y)
            isfile(failfile) && (try rm(failfile); true catch; false end) && error("injected failure")
            x .+= y
            nothing
        end
    end

    tmpdir = mktempdir(;cleanup=false)
    options = SchedulerOptions(;scratch=tmpdir, reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=64, reduce_partbytes=128, reduce_partcopies=2, reducer_chunk! = flaky_copy_chunk!)
    logger = Test.TestLogger(; min_level=Logging.Debug, catch_exceptions=true)
    r,tsks = with_logger(logger) do
        epmapreduce!(zeros(Float32, 100), options, foo_failed_copy, 1:6)
    end
    rmprocs(workers())
    msgs = [string(l.message) for l in logger.logs]

    nstarted = count(m->startswith(m, "splitting the merge") || startswith(m, "reducing into checkpoint3"), msgs)
    ncommitted = count(m->startswith(m, "pushed merged checkpoint3") || startswith(m, "pushed reduced checkpoint3"), msgs)
    # a copy of a part that fails, or that fails after another copy of its part finished (and so lost)
    part_failed = any(m->occursin("caught exception during part", m) || occursin(r"^copy \d+ of part \d+ of \d+ into checkpoint3 lost, pid=\d+, failed", m), msgs)
    @test !isfile(failfile)   # the failure was injected
    @test all(==(sum(1:6)), r)
    if part_failed
        @test nstarted == ncommitted # another copy of the part did it, so no merge was done again
    else
        # the failure hit a merge that ran whole, as when no other worker was idle as it started: it is done again
        @test any(m->occursin("caught exception during reduce", m), msgs) && nstarted == ncommitted + 1
    end
    @test isempty(readdir(tmpdir))
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, streaming reduction, splitting a merge that runs whole" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers
    @everywhere begin
        foo_resplit(r, tsk) = (r .+= tsk; sleep(tsk <= 2 ? 0.2 : 2); nothing)
        # slow enough that the first merge, which runs whole, still runs once the map is done
        chunk_resplit!(x, y) = (sleep(1); x .+= y; nothing)
    end

    # one reducer at a time while the map runs, so that the first merge runs whole, and more once it is done, so that the merge can be split
    maxreduce = Ref(1)
    tmpdir = mktempdir(;cleanup=false)
    options = SchedulerOptions(;scratch=tmpdir, reduce_streaming=true, reduce_fanin=2, reduce_chunkbytes=32, reduce_partbytes=200, reduce_resplit=true,
        reducer_chunk! = chunk_resplit!, maxreduceworkers = ()->maxreduce[], reduce_trigger = eloop->(Schedulers.spare_work_allowed(eloop) && (maxreduce[] = 10)))
    logger = Test.TestLogger(; min_level=Logging.Debug, catch_exceptions=true)
    r,tsks = with_logger(logger) do
        epmapreduce!(zeros(Float32, 200), options, foo_resplit, 1:3)
    end
    rmprocs(workers())
    msgs = [string(l.message) for l in logger.logs]

    @test all(==(sum(1:3)), r)
    @test any(m->startswith(m, "also splitting the running merge"), msgs)
    @test any(m->occursin(r"^copy \d+ of (the whole merge|part \d+ of \d+) into checkpoint3 lost", m), msgs) # the whole run, or the parts, lost
    @test isempty(readdir(tmpdir))
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, partial reduction, streaming, copies of parts" begin
    using Distributed, Schedulers

    function foo_copies_trigger(r, i)
        r .+= i
        sleep(2)
        nothing
    end

    function trigger_copies(eloop, ntasks)
        if length(complete_tasks(eloop)) - ntasks[] >= 10
            trigger_reduction!(eloop)
            ntasks[] = length(complete_tasks(eloop))
        end
    end

    tmpdir = mktempdir(;cleanup=false)
    tmpfile = tempname()

    ntasks = Ref(0)
    options = SchedulerOptions(;maxworkers=10, scratch=tmpdir, reduce_trigger=eloop->trigger_copies(eloop, ntasks), save_partial_reduction=input->write(tmpfile, input),
        reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=16, reduce_partbytes=16, reduce_partcopies=2, reduce_resplit=true)
    t = @async epmapreduce!(zeros(10), options, foo_copies_trigger, 1:20)
    @test timedwait(()->istaskdone(t), 300.0; pollint=1.0) == :ok

    if istaskdone(t)
        r,tsks = fetch(t)
        @test r ≈ sum([1:20;]) * ones(10)
        x = read!(tmpfile, zeros(10))
        @test x[1] >= sum([1:10;])
        @test all(≈(x[1]), x)
        rm(tmpfile; force=true)
    end
    @test isempty(readdir(tmpdir))
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, a worker that is being removed gets no more work" begin
    safe_addprocs(4)
    @everywhere using Distributed, Schedulers
    badpid = workers()[1]
    @everywhere function foo_removed(r, tsk, badpid)
        if myid() == badpid
            # hold up the removal of this worker, as while other workers join the cluster: rmprocs waits for this lock
            remotecall_fetch(1) do
                @async begin
                    lock(Distributed.worker_lock)
                    try
                        sleep(5)
                    finally
                        unlock(Distributed.worker_lock)
                    end
                end
                nothing
            end
            error("this worker is bad")
        end
        r .+= tsk
        sleep(1)
        nothing
    end

    tmpdir = mktempdir(;cleanup=false)
    options = SchedulerOptions(;scratch=tmpdir, reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=64, reduce_partbytes=128)
    logger = Test.TestLogger(; min_level=Logging.Debug, catch_exceptions=true)
    r,tsks = with_logger(logger) do
        epmapreduce!(zeros(Float32, 100), options, foo_removed, 1:12, badpid)
    end
    msgs = [string(l.message) for l in logger.logs]

    @test all(==(sum(1:12)), r)
    # the bad worker mapped once, failed, and got no more tasks, merges or parts while it was removed
    @test count(==("map, pid=$badpid"), msgs) == 1
    @test count(==("reduce, pid=$badpid"), msgs) == 0
    @test badpid ∉ workers()
    rmprocs(workers())
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, a worker that does not answer its lookup does not hold up the end" begin
    safe_addprocs(4)
    @everywhere using Distributed, Schedulers
    slowpid = workers()[1]
    @everywhere slowpid = $slowpid
    @everywhere begin
        lookups_slow = Ref(0)
        # the first lookup of `slowpid`, as it starts to map, hangs, as for a worker that does not respond
        gethostname_slow() = (myid() == slowpid && (lookups_slow[] += 1) == 1 && sleep(50); gethostname())
        foo_lookup(r, tsk) = (r .+= tsk; sleep(1); nothing)
    end

    tmpdir = mktempdir(;cleanup=false)
    options = SchedulerOptions(;scratch=tmpdir, gethostname=gethostname_slow, reduce_streaming=true, reduce_fanin=3, reduce_chunkbytes=64)
    elapsed = @elapsed r,tsks = epmapreduce!(zeros(Float32, 100), options, foo_lookup, 1:12)

    @test all(==(sum(1:12)), r)
    @test elapsed < 40         # the lookup is given up once there are no tasks left, without waiting it out
    @test slowpid ∈ workers()  # and the worker is not taken to be bad
    rmprocs(workers())
    rm(tmpdir; recursive=true, force=true)
end

@testset "removing a checkpoint does not time out in less than a minute" begin
    # removing a checkpoint usually takes a fraction of a second, which is timed as a second
    times = [300.0; fill(1.0, 20)]
    @test Schedulers.maximum_task_time(times, 10, 5) == 5.0
    @test Schedulers.maximum_task_time(Schedulers.TimesAtLeast(times, 60.0, 5.0), 10, 5) == 60.0
    # longer usual times still give longer timeouts
    @test Schedulers.maximum_task_time(Schedulers.TimesAtLeast([300.0; fill(20.0, 20)], 60.0, 5.0), 10, 5) == 100.0
    # and times are recorded as they are
    t = Schedulers.TimesAtLeast(Float64[], 60.0, 5.0)
    push!(t, 2.0)
    @test t.times == [2.0]

    # a call that takes longer than a few seconds, but less than a minute, does not time out
    safe_addprocs(1)
    @everywhere using Distributed, Schedulers
    @everywhere sleep_rm(seconds) = (sleep(seconds); nothing)
    @test Schedulers.remotecall_wait_timeout(Schedulers.TimesAtLeast([300.0; fill(1.0, 20)], 60.0, 5.0), 10, 5.0, nothing, tsk->nothing, tsk->nothing, 0, sleep_rm, workers()[1], 7) === nothing
    rmprocs(workers())
end

@testset "pmapreduce, assigning the result to a global at the top level of a script" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers
    @everywhere foo_toplevel(r, tsk) = (r .+= tsk; nothing)

    # since Julia 1.12, the global is declared, but not assigned, while epmapreduce! runs, and starting the workers copies the globals of Main to them
    tmpdir = mktempdir(;cleanup=false)
    options = SchedulerOptions(;scratch=tmpdir)
    t = @async Core.eval(Main, :(schedulers_test_result = epmapreduce!(zeros(10), $options, foo_toplevel, 1:4)))
    @test timedwait(()->istaskdone(t), 120.0; pollint=1.0) == :ok # the workers start, instead of failing to
    if istaskdone(t)
        @test !istaskfailed(t)
        @test Main.schedulers_test_result[1] ≈ sum(1:4) * ones(10)
    end
    rmprocs(workers())
    rm(tmpdir; recursive=true, force=true)
end

@testset "copy SchedulerOptions" begin
    options = SchedulerOptions()
    _options = copy(options)

    for fieldname in fieldnames(SchedulerOptions)
        @test getfield(options, fieldname) == getfield(_options, fieldname)
    end
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

@testset "epmapreduce! hostname lookup failure" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random
    @everywhere function foo6(x, tsk, a; b)
        x .+= a*b*tsk
        nothing
    end

    tmpdir = mktempdir(;cleanup=false)

    a,b = 2,3
    options = SchedulerOptions(;scratch=tmpdir, gethostname=()->rand() > 0.8 ? error("error") : gethostname())
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo6, 1:100, a; b=b)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:100;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "epmapreduce! no tasks completed" begin
    tmpdir = mktempdir(;cleanup=false)

    function foo7(x, tsk, iter)
        @show iter
        if iter < 3
            sleep(2)
        else
            sleep(10)
        end
        error("I don't want the task to complete, iteration=$iter")
    end

    options = SchedulerOptions(;scratch=tmpdir, skip_tasks_that_timeout=true, minworkers=0, maxworkers=1, timeout_function_multiplier=2)

    global it = Ref{Int}(1)
    function getiter(it)
        it[] = it[] + 1
    end
    x,tsks = epmapreduce!(zeros(Float32,10), options, (x,i)->(getiter(it); foo7(x, i, it[])), 1:1)
    rmprocs(workers())
    rm(tmpdir; recursive=true, force=true)
    @test x ≈ zeros(Float32, 10)
    @test tsks == [1]
end

@testset "epmapreduce! slow/late task termination" begin
    safe_addprocs(5)

    @everywhere function fg!(g, ishot, c)
        tsk_len = [30, 1,  15, 1]
        tsk_num = [1,  10, 1, 10]
        timetable = vcat([fill(tsk_len[i], tsk_num[i]) for i in 1:length(tsk_len)]...)
        sleep(timetable[ishot])
        g .+= ishot * c
        nothing
    end

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(;
        scratch=tmpdir,
        maxworkers = 5,
        timeout_function_multiplier = 2.,
        null_tsk_runtime_threshold = 0.1,
        skip_tsk_tol_ratio = 0.3,
        grace_period_ratio = 0.1,
        skip_tasks_that_timeout = true,
    )

    g = zeros(Float32, 5)
    g, tsks = epmapreduce!(g, options, fg!, 1 : 22, 2)
    rmprocs(workers())
    rm(tmpdir; recursive=true, force=true)

    @test 1 ∈ tsks
    @test 12 ∈ tsks
    # besides the slow tasks, only short tasks still running at the end of the grace period can be skipped, those that are
    # checked before they finish, which depends on timing
    @test all(tsk->tsk ∈ (1, 12) || tsk > 12, tsks)
    @test g == ones(size(g)) .* (sum([1 : 22;]) - sum(tsks)) * 2
end

@testset "minworkers > maxworkers" begin
    options = SchedulerOptions(;minworkers=10, maxworkers=5)

    tsk = @async epmap(SchedulerOptions(;minworkers=10, maxworkers=5), x->2 .* x, 1:10)

    while !istaskdone(tsk)
        @test nworkers() <= 5
        sleep(0.01)
    end
end
