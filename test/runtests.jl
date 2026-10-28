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
end
MemoryCheckpoint(data; onread=offset->nothing, onwrite=offset->nothing) = MemoryCheckpoint(data, onread, onwrite, false, false)
Base.filesize(c::MemoryCheckpoint) = length(c.data)
Schedulers.checkpoint_read!(c::MemoryCheckpoint, buffer, offset) = (c.onread(offset); copyto!(buffer, 1, c.data, offset + 1, length(buffer)); nothing)
Schedulers.checkpoint_write_chunk(c::MemoryCheckpoint, ichunk, nchunks, buffer, offset) = (c.onwrite(offset); copyto!(c.data, offset + 1, buffer, 1, length(buffer)); nothing)
Schedulers.checkpoint_commit(c::MemoryCheckpoint, nchunks) = (c.committed = true; nothing)
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
    @test length(tsks) > 2
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
