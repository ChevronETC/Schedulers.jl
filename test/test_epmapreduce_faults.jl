using Distributed, Logging, Random, Schedulers, Serialization, Test

ENV["JULIA_WORKER_TIMEOUT"] = "120"

function safe_addprocs(n)
    try
        length(addprocs(n))
    catch e
        Schedulers.logerror(e)
        @warn "problem calling addprocs, nworkers=$(nworkers())"
    end
end

@testset "epmapreduce fault tests" begin

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
    tsk = @async epmapreduce!(zeros(Float32,10), options, foo7, 1:20, a, b)

    sleep(5)
    rmprocs(workers()[randperm(nworkers())[1]])

    x,tsks = fetch(tsk)
    @test nworkers() == 5
    _wrkrs = workers()
    @test wrkrs != _wrkrs

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:20;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with ErrorException during checkpoint" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers

    # Global counters: fail the first N saves/loads total, then succeed.
    save_counter = RemoteChannel(()->Channel{Int}(1))
    put!(save_counter, 0)
    load_counter = RemoteChannel(()->Channel{Int}(1))
    put!(load_counter, 0)

    @everywhere function foo7a(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(0.1)
        nothing
    end

    @everywhere function test_save_checkpoint_a(checkpoint, localresult, save_counter)
        n = take!(save_counter) + 1
        put!(save_counter, n)
        if n <= 3
            error("deterministic save failure #$n")
        end
        Schedulers.default_save_checkpoint(checkpoint, localresult)
    end

    @everywhere function test_load_checkpoint_a(checkpoint, load_counter)
        n = take!(load_counter) + 1
        put!(load_counter, n)
        if n <= 2
            error("deterministic load failure #$n")
        end
        Schedulers.default_load_checkpoint(checkpoint)
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(;maxworkers=5, scratch=tmpdir,
        load_checkpoint=(cp)->test_load_checkpoint_a(cp, load_counter),
        save_checkpoint=(cp, lr)->test_save_checkpoint_a(cp, lr, save_counter),
        retries=0)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo7a, 1:20, a, b)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:20;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with ErrorException during checkpoint and retries=1" begin
    # important to test with retries=1 since we need to ensure that we don't reduce things twice
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers

    # Global counters: fail the first N saves/loads total, then succeed.
    save_counter = RemoteChannel(()->Channel{Int}(1))
    put!(save_counter, 0)
    load_counter = RemoteChannel(()->Channel{Int}(1))
    put!(load_counter, 0)

    @everywhere function foo7b(x, tsk, a, b)
        fetch(x)::Vector{Float32} .+= a*b*tsk
        sleep(0.1)
        nothing
    end

    @everywhere function test_save_checkpoint_b(checkpoint, localresult, save_counter)
        n = take!(save_counter) + 1
        put!(save_counter, n)
        if n <= 3
            error("deterministic save failure #$n")
        end
        Schedulers.default_save_checkpoint(checkpoint, localresult)
    end

    @everywhere function test_load_checkpoint_b(checkpoint, load_counter)
        n = take!(load_counter) + 1
        put!(load_counter, n)
        if n <= 2
            error("deterministic load failure #$n")
        end
        Schedulers.default_load_checkpoint(checkpoint)
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(;maxworkers=5, scratch=tmpdir,
        load_checkpoint=(cp)->test_load_checkpoint_b(cp, load_counter),
        save_checkpoint=(cp, lr)->test_save_checkpoint_b(cp, lr, save_counter),
        retries=1)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo7b, 1:20, a, b)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:20;]) * ones(10)
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
        if tsk ∈ (17,18,19,20)
            sleep(10) # this should force the reduction to start before all tasks are finished.
        else
            sleep(1)
        end
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(;maxworkers=5, scratch=tmpdir, retries=0, timeout_function_multiplier=15)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo7c, 1:20, a, b)

    rmprocs(workers())

    @test x ≈ sum(a*b*[1:20;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, cluster with RemoteException during tasks" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers

    # Track which tasks have already failed once so they succeed on retry.
    failed_once = RemoteChannel(()->Channel{Set{Int}}(1))
    put!(failed_once, Set{Int}())

    @everywhere function foo8(x, tsk, a, b, failed_once)
        s = take!(failed_once)
        if tsk ∈ (2, 5, 8) && tsk ∉ s
            push!(s, tsk)
            put!(failed_once, s)
            error("deterministic first-attempt failure for task $tsk")
        end
        put!(failed_once, s)

        x .+= a*b*tsk
        sleep(0.1)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(;minworkers=5, maxworkers=5, scratch=tmpdir, retries=1, maxerrors=typemax(Int))
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo8, 1:10, a, b, failed_once)

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
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo17, 1:20, a; b=b)
    rm(joinpath(tempdir(), "touch.txt"))
    @test isempty(tsks)

    ncheckpoints = [length(readdir(tmpdir)) for tmpdir in tmpdirs]
    ncheckpoints_average = sum(ncheckpoints) / 3
    for i = 1:3
        @test ncheckpoints[i] > 0
        @test (ncheckpoints[i] - ncheckpoints_average) < .1*ncheckpoints_average
    end

    rmprocs(workers())
    @test x ≈ (sum(a*b*[1:20;])) * ones(10)
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
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo17, 1:20, a; b=b)
    @test tsks == [20]

    ncheckpoints = [length(readdir(tmpdir)) for tmpdir in tmpdirs]
    ncheckpoints_average = sum(ncheckpoints) / 3
    for i = 1:3
        @test ncheckpoints[i] > 0
        @test (ncheckpoints[i] - ncheckpoints_average) < .1*ncheckpoints_average
    end

    rmprocs(workers())
    @test x ≈ (sum(a*b*[1:20;]) - a*b*20) * ones(10)
    rm.(tmpdirs; recursive=true, force=true)
end

@testset "pmapreduce with task checkpoint and restart" begin
    s = Dict{Int,Future}()
    p = Dict{Int,Future}()
    lk = Dict{Int,Future}()

    function init(pid, s)
        s[pid] = remotecall(ones, pid, Int, 1)
        p[pid] = remotecall(Channel{Bool}, pid, 1)
        lk[pid] = remotecall(ReentrantLock, pid)
    end

    r = randstring('a':'z', 6)

    function foo18(result, tsk, s, lk, r)
        _s = fetch(s[myid()])::Vector{Int}
        _lk = fetch(lk[myid()])::ReentrantLock
        for i = _s[1]:10
            lock(_lk) do
                _s .= i
            end
            touch("testfile-$r-$tsk-$i.txt")
            @info "_s on pid=$(myid()) is $(_s[1])"
            sleep(5)
        end
        lock(_lk) do
            result .+= _s[1]
            _s .= 1
        end
    end

    function checkpoint_task(tsk, s, lk)
        @info "checkpoint task..."
        _s = fetch(s[myid()])::Vector{Int}
        _lk = fetch(lk[myid()])::ReentrantLock
        lock(_lk) do
            write("task_checkpoint_$tsk.bin", _s)
            @info "...checkpoint task with state=$(_s[1])."
        end
    end

    function restart_task!(tsk, s, lk)
        @info "restart task..."
        _s = fetch(s[myid()])::Vector{Int}
        _lk = fetch(lk[myid()])::ReentrantLock
        lock(_lk) do
            if isfile("task_checkpoint_$tsk.bin")
                read!("task_checkpoint_$tsk.bin", _s)
                rm("task_checkpoint_$tsk.bin")
            end
            @info "...restart task, " _s
        end
    end

    function signal_preempt(p)
        put!(fetch(p[myid()]), true)
    end

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(;
        minworkers = 0,
        maxworkers = 2,
        init = pid->init(pid, s),
        checkpoint_task = tsk->checkpoint_task(tsk, s, lk),
        restart_task = tsk->restart_task!(tsk, s, lk),
        preempt_channel_future = pid->p[pid],
        scratch = tmpdir
    )

    t = @async epmapreduce!(zeros(2), options, (result,i)->foo18(result,i,s,lk,r), 1:4)

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

end # @testset "epmapreduce fault tests"
