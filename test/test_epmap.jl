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

@testset "epmap tests" begin

@testset "pmap, stable cluster test" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo1(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk,$(myid())")
        sleep(1)
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
        sleep(2)
    end

    options = SchedulerOptions(;maxworkers=10,minworkers=10)
    epmap(options, foo2, 1:20, s)

    h = Dict()
    for w in workers()
        h[w] = 0
    end

    @test nworkers() > 5
    rmprocs(workers())

    for tsk = 1:20
        r = read(joinpath(tempdir(), "task-$s-$tsk.txt"), String)
        r_tsk, r_pid = split(r, ",")
        h[parse(Int,r_pid)] += 1
        @test r_tsk == "$tsk"
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end

    for (key,value) in h
        @test value ∈ 0:10
    end
end

@testset "pmap, elastic cluster with faults" begin
    safe_addprocs(10)
    wrkrs = workers()
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo3(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
        sleep(2)
    end
    options = SchedulerOptions(;maxworkers=10)
    tsk = @async epmap(options, foo3, 1:20, s)

    sleep(5)
    faulty_pids = workers()[randperm(length(workers()))[1:2]]
    rmprocs(faulty_pids)

    wait(tsk)
    _wrkrs = workers()
    @test wrkrs != _wrkrs

    h = Dict()

    rmprocs(workers())

    for tsk = 1:20
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
            @test value ∈ 1:10
        end
    end
end

@testset "pmap with shrinking cluster" begin
    safe_addprocs(10)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo4(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
        sleep(2)
    end

    h = Dict()
    for w in workers()
        h[w] = 0
    end

    options = SchedulerOptions(;maxworkers=10, minworkers=4)
    epmap(options, foo4, 1:20, s)

    @test nworkers() == 4
    rmprocs(workers())

    for tsk = 1:20
        r = read(joinpath(tempdir(), "task-$s-$tsk.txt"), String)
        r_tsk, r_pid = split(r, ",")
        h[parse(Int,r_pid)] += 1
        @test r_tsk == "$tsk"
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end

    for (key,value) in h
        @test value ∈ 1:10
    end
end

@testset "pmap with interactive growing cluster" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo5(tsk, s)
        write(joinpath(tempdir(), "task-$s-$tsk.txt"), "$tsk, $(myid())")
        sleep(2)
    end

    _nworkers = 5

    options = SchedulerOptions(;maxworkers=()->_nworkers, minworkers=()->_nworkers)
    tsk = @async epmap(options, foo5, 1:20, s)

    sleep(5)
    _nworkers = 10

    wait(tsk)
    @test nworkers() > 5

    h = Dict()
    for w in workers()
        h[w] = 0
    end

    rmprocs(workers())

    for tsk = 1:20
        r = read(joinpath(tempdir(), "task-$s-$tsk.txt"), String)
        r_tsk, r_pid = split(r, ",")
        h[parse(Int,r_pid)] += 1
        @test r_tsk == "$tsk"
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end

    for (key,value) in h
        @test value ∈ 0:10
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
    epmap(options, foo5b, 1:20, s)

    h = Dict()
    for w in workers()
        h[w] = 0
    end

    rmprocs(workers())

    for tsk = 1:20
        r = read(joinpath(tempdir(), "task-$s-$tsk.txt"), String)
        r_tsk, r_pid = split(r, ",")
        h[parse(Int,r_pid)] += 1
        @test r_tsk == "$tsk"
        rm(joinpath(tempdir(), "task-$s-$tsk.txt"))
    end

    for (key,value) in h
        @test value ∈ 1:20
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

@testset "epmap convenience form (no options)" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo_conv(tsk, s)
        write(joinpath(tempdir(), "task-conv-$s-$tsk.txt"), "$tsk")
    end
    epmap(foo_conv, 1:4, s)
    for tsk in 1:4
        @test isfile(joinpath(tempdir(), "task-conv-$s-$tsk.txt"))
        rm(joinpath(tempdir(), "task-conv-$s-$tsk.txt"))
    end
    rmprocs(workers())
end

@testset "epmap with journalfile" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo_journal(tsk, s)
        write(joinpath(tempdir(), "task-journal-$s-$tsk.txt"), "$tsk")
    end
    tmpfile = tempname() * ".json"
    options = SchedulerOptions(;journalfile=tmpfile, maxworkers=2)
    journal, tsks = epmap(options, foo_journal, 1:4, s)
    @test haskey(journal, "tasks")
    @test haskey(journal, "done")
    for tsk in 1:4
        rm(joinpath(tempdir(), "task-journal-$s-$tsk.txt"); force=true)
    end
    rm(tmpfile; force=true)
    rmprocs(workers())
end

@testset "epmap with usemaster=true" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers
    s = randstring(6)
    @everywhere function foo_master(tsk, s)
        write(joinpath(tempdir(), "task-master-$s-$tsk.txt"), "$tsk,$(myid())")
    end
    options = SchedulerOptions(;usemaster=true, maxworkers=2)
    epmap(options, foo_master, 1:6, s)

    pids_used = Set{Int}()
    for tsk in 1:6
        r = read(joinpath(tempdir(), "task-master-$s-$tsk.txt"), String)
        _, pid_str = split(r, ",")
        push!(pids_used, parse(Int, pid_str))
        rm(joinpath(tempdir(), "task-master-$s-$tsk.txt"))
    end
    @test 1 ∈ pids_used
    rmprocs(workers())
end

end # @testset "epmap tests"
