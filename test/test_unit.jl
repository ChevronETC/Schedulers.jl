using Distributed, Logging, Random, Schedulers, Serialization, Test

@testset "Unit Tests" begin

@testset "now_formatted" begin
    ts = Schedulers.now_formatted()
    @test occursin(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", ts)
end

@testset "logerror" begin
    # Already tested in runtests.jl but let's cover the loglevel default
    try
        error("test error for logerror")
    catch e
        # Default loglevel (Debug) — should not error
        io = IOBuffer()
        with_logger(ConsoleLogger(io, Logging.Debug)) do
            Schedulers.logerror(e)
        end
        s = String(take!(io))
        @test contains(s, "test error for logerror")
    end
end

@testset "robust_average" begin
    # Not enough tasks completed (< 30% of tsk_count) → Inf
    @test Schedulers.robust_average([1.0, 2.0, 3.0], 0.0, 100, 1, false) == Inf

    # Enough tasks completed (30% of 10 = 3, we have 3 robust times)
    avg = Schedulers.robust_average([1.0, 2.0, 3.0], 0.0, 10, 1, false)
    @test avg ≈ 2.0

    # With null_tsk_runtime_threshold filtering out short tasks
    avg = Schedulers.robust_average([0.01, 0.02, 1.0, 2.0, 3.0], 0.5, 10, 1, false)
    @test avg ≈ 2.0  # only 1.0, 2.0, 3.0 pass threshold; 3/10 >= 0.3

    # All tasks below threshold → Inf (empty robust set)
    @test Schedulers.robust_average([0.01, 0.02], 0.5, 10, 1, false) == Inf

    # Empty tsk_times → Inf
    @test Schedulers.robust_average(Float64[], 0.0, 10, 1, false) == Inf

    # Exactly at the 30% boundary (3 out of 10)
    avg = Schedulers.robust_average([5.0, 5.0, 5.0], 0.0, 10, 1, false)
    @test avg ≈ 5.0

    # Below the 30% boundary (2 out of 10)
    @test Schedulers.robust_average([5.0, 5.0], 0.0, 10, 1, false) == Inf

    # With report=true (exercises @debug path)
    avg = Schedulers.robust_average([1.0, 2.0, 3.0], 0.0, 10, 1, true)
    @test avg ≈ 2.0
end

@testset "check_timeout_status" begin
    # No timeout: tic is recent, no tasks completed, robust_average returns Inf
    tic = time()
    @test Schedulers.check_timeout_status(tic, Float64[], 10, 5.0, Inf, 0.0, 0.0, 1, false) == false

    # Function timeout: elapsed > average * multiplier
    # average = 1.0 (3 times of 1.0, tsk_count=10), multiplier=2.0
    # If tic is 3 seconds ago, elapsed=3 > 1.0*2.0=2.0 → timeout
    tic = time() - 3.0
    @test Schedulers.check_timeout_status(tic, [1.0, 1.0, 1.0], 10, 2.0, Inf, 0.0, 0.0, 1, false) == true

    # No function timeout but grace period timeout
    # tic is recent (no function timeout), but grace_period_start_time is old
    tic = time()
    grace_start = time() - 10.0
    # average = 1.0, grace_period_ratio = 5.0, so threshold = 5.0; elapsed since grace = 10 > 5 → timeout
    @test Schedulers.check_timeout_status(tic, [1.0, 1.0, 1.0], 10, 100.0, grace_start, 0.0, 5.0, 1, false) == true

    # No timeout at all: everything within bounds
    tic = time()
    @test Schedulers.check_timeout_status(tic, [1.0, 1.0, 1.0], 10, 100.0, Inf, 0.0, 0.0, 1, false) == false

    # With report=true
    tic = time() - 3.0
    @test Schedulers.check_timeout_status(tic, [1.0, 1.0, 1.0], 10, 2.0, Inf, 0.0, 0.0, 1, true) == true
end

@testset "maximum_task_time" begin
    # Not enough tasks → Inf
    @test Schedulers.maximum_task_time(Float64[], 10, 5.0) == Inf

    # Enough tasks (more than floor(0.5*10)=5)
    times = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    @test Schedulers.maximum_task_time(times, 10, 2.0) ≈ 12.0  # max=6, multiplier=2

    # Exactly at threshold: length=5, floor(0.5*10)=5, 5 > 5 is false → Inf
    times5 = [1.0, 2.0, 3.0, 4.0, 5.0]
    @test Schedulers.maximum_task_time(times5, 10, 2.0) == Inf

    # Just over threshold
    times6 = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    @test Schedulers.maximum_task_time(times6, 10, 3.0) ≈ 18.0
end

@testset "TimeoutException" begin
    e = Schedulers.TimeoutException(42, 123.4)
    @test e.pid == 42
    @test e.elapsed ≈ 123.4
end

@testset "PreemptException" begin
    e = Schedulers.PreemptException()
    @test e isa Exception
end

@testset "handle_exception - PreemptException" begin
    fails = Dict(1 => 0)
    r = Schedulers.handle_exception(Schedulers.PreemptException(), 1, "host1", fails, 10, 3)
    @test r.bad_pid == true
    @test r.do_break == true
    @test r.do_interrupt == false
    @test r.do_error == true
    # PreemptException should NOT increment fails
    @test fails[1] == 0
end

@testset "handle_exception - TimeoutException" begin
    fails = Dict(1 => 0)
    r = Schedulers.handle_exception(Schedulers.TimeoutException(1, 5.0), 1, "host1", fails, 10, 3)
    @test r.bad_pid == true
    @test r.do_break == true
    @test r.do_interrupt == false
    @test r.do_error == false
    @test fails[1] == 1

    # Trigger maxerrors
    fails2 = Dict(1 => 9)
    r2 = Schedulers.handle_exception(Schedulers.TimeoutException(1, 5.0), 1, "host1", fails2, 10, 3)
    @test r2.do_interrupt == true
    @test r2.do_error == true
end

@testset "handle_exception - InterruptException" begin
    fails = Dict(1 => 0)
    r = Schedulers.handle_exception(InterruptException(), 1, "host1", fails, 10, 3)
    @test r.bad_pid == false
    @test r.do_break == false
    @test r.do_interrupt == true
    @test r.do_error == false
end

@testset "handle_exception - ProcessExitedException" begin
    fails = Dict(1 => 0)
    e = ProcessExitedException(1)
    r = Schedulers.handle_exception(e, 1, "host1", fails, 10, 3)
    @test r.bad_pid == true
    @test r.do_break == true
    @test r.do_interrupt == false
    @test r.do_error == false
end

@testset "handle_exception - generic with retries exceeded" begin
    fails = Dict(1 => 3)
    r = Schedulers.handle_exception(ErrorException("test"), 1, "host1", fails, 100, 3)
    @test r.bad_pid == true
    @test r.do_break == true
    @test r.do_interrupt == false
    @test r.do_error == false
    @test fails[1] == 4  # incremented

    # Generic error within retry count — no action
    fails2 = Dict(1 => 0)
    r2 = Schedulers.handle_exception(ErrorException("test"), 1, "host1", fails2, 100, 3)
    @test r2.bad_pid == false
    @test r2.do_break == false
    @test r2.do_interrupt == false
    @test r2.do_error == false
end

@testset "handle_exception - generic with maxerrors exceeded" begin
    fails = Dict(1 => 0, 2 => 9)
    r = Schedulers.handle_exception(ErrorException("test"), 1, "host1", fails, 10, 100)
    @test r.do_break == true
    @test r.do_interrupt == true
    @test r.do_error == true
end

@testset "handle_exception - TaskFailedException unwrap" begin
    # Create a failed task
    t = @async error("wrapped error")
    try; wait(t); catch; end
    e = TaskFailedException(t)
    fails = Dict(1 => 0)
    r = Schedulers.handle_exception(e, 1, "host1", fails, 100, 3)
    # Should unwrap and handle the inner ErrorException
    @test fails[1] == 1
end

@testset "default_reducer!" begin
    x = [1.0, 2.0, 3.0]
    y = [4.0, 5.0, 6.0]
    Schedulers.default_reducer!(x, y)
    @test x ≈ [5.0, 7.0, 9.0]
end

@testset "default_save_checkpoint and default_load_checkpoint" begin
    tmpfile = tempname()
    data = [1.0, 2.0, 3.0]
    Schedulers.default_save_checkpoint(tmpfile, data)
    loaded = Schedulers.default_load_checkpoint(tmpfile)
    @test loaded ≈ data
    rm(tmpfile)
end

@testset "default_rm_checkpoint" begin
    # File exists → should remove
    tmpfile = tempname()
    write(tmpfile, "test")
    @test isfile(tmpfile)
    Schedulers.default_rm_checkpoint(tmpfile)
    @test !isfile(tmpfile)

    # File doesn't exist → should not error
    Schedulers.default_rm_checkpoint(tmpfile)
end

@testset "next_checkpoint_id and next_scratch_index" begin
    # next_checkpoint_id returns incrementing ids
    id1 = Schedulers.next_checkpoint_id()
    id2 = Schedulers.next_checkpoint_id()
    @test id2 == id1 + 1

    # next_scratch_index cycles through indices
    idx1 = Schedulers.next_scratch_index(3)
    idx2 = Schedulers.next_scratch_index(3)
    idx3 = Schedulers.next_scratch_index(3)
    @test idx1 ∈ 1:3
    @test idx2 ∈ 1:3
    @test idx3 ∈ 1:3
end

@testset "next_checkpoint" begin
    tmpdir = mktempdir()
    cp = Schedulers.next_checkpoint("testid", [tmpdir])
    @test startswith(cp, tmpdir)
    @test contains(cp, "checkpoint-testid-")
    rm(tmpdir; recursive=true)
end

@testset "next_checkpoint with multiple scratch" begin
    tmpdirs = [mktempdir() for _ in 1:3]
    checkpoints = [Schedulers.next_checkpoint("multi", tmpdirs) for _ in 1:6]
    # Should distribute across scratch locations
    dirs_used = Set(dirname(cp) for cp in checkpoints)
    @test length(dirs_used) > 1
    rm.(tmpdirs; recursive=true)
end

@testset "journal_init" begin
    # Without reduce
    journal = Schedulers.journal_init(1:3, tsks->nothing; reduce=false)
    @test haskey(journal, "tasks")
    @test !haskey(journal, "checkpoints")
    @test haskey(journal, "start")
    @test length(journal["tasks"]) == 3

    # With reduce
    journal_r = Schedulers.journal_init(1:3, tsks->nothing; reduce=true)
    @test haskey(journal_r, "tasks")
    @test haskey(journal_r, "checkpoints")
    @test haskey(journal_r, "rmcheckpoints")
    @test haskey(journal_r, "pids")
end

@testset "journal_final" begin
    journal = Schedulers.journal_init(1:2, tsks->nothing; reduce=true)
    # Add a pid entry with a tic
    journal["pids"]["42"] = Dict("tic" => time(), "restart" => Dict("elapsed" => 0.0, "faults" => 0))
    Schedulers.journal_final(journal)
    @test haskey(journal, "done")
    @test !haskey(journal["pids"]["42"], "tic")  # tic should be removed
end

@testset "journal_write" begin
    journal = Schedulers.journal_init(1:2, tsks->nothing; reduce=false)
    Schedulers.journal_final(journal)

    # Empty filename — should not write anything
    Schedulers.journal_write(journal, "")

    # Non-empty filename — should write JSON
    tmpfile = tempname()
    Schedulers.journal_write(journal, tmpfile)
    @test isfile(tmpfile)
    content = read(tmpfile, String)
    @test contains(content, "tasks")
    @test contains(content, "start")
    @test contains(content, "done")
    rm(tmpfile)
end

@testset "journal_start! and journal_stop! - tasks stage" begin
    journal = Schedulers.journal_init(1:3, tsks->nothing; reduce=false)
    Schedulers.journal_start!(journal; stage="tasks", tsk=1, pid=42, hostname="testhost")
    @test length(journal["tasks"][1]["trials"]) == 1
    trial = journal["tasks"][1]["trials"][1]
    @test trial["pid"] == 42
    @test trial["hostname"] == "testhost"
    @test haskey(trial, "start")

    Schedulers.journal_stop!(journal; stage="tasks", tsk=1, pid=42, fault=false)
    @test trial["status"] == "succeeded"
    @test haskey(trial, "stop")

    # With fault=true
    Schedulers.journal_start!(journal; stage="tasks", tsk=2, pid=43, hostname="testhost2")
    Schedulers.journal_stop!(journal; stage="tasks", tsk=2, pid=43, fault=true)
    @test journal["tasks"][2]["trials"][1]["status"] == "failed"
end

@testset "journal_start! and journal_stop! - checkpoints stage" begin
    journal = Schedulers.journal_init(1:3, tsks->nothing; reduce=true)
    Schedulers.journal_start!(journal; stage="checkpoints", tsk=1, pid=42, hostname="testhost")
    @test length(journal["checkpoints"][1]["trials"]) == 1

    Schedulers.journal_stop!(journal; stage="checkpoints", tsk=1, pid=42, fault=false)
    @test journal["checkpoints"][1]["trials"][1]["status"] == "succeeded"
end

@testset "journal_start! and journal_stop! - restart/reduce stages" begin
    journal = Schedulers.journal_init(1:3, tsks->nothing; reduce=true)

    # restart stage — should create pid entry
    Schedulers.journal_start!(journal; stage="restart", tsk=0, pid="10", hostname="host1")
    @test haskey(journal["pids"], "10")
    @test haskey(journal["pids"]["10"], "restart")
    @test haskey(journal["pids"]["10"], "reduce")
    @test journal["pids"]["10"]["hostname"] == "host1"

    # Stop restart stage — should update elapsed
    Schedulers.journal_stop!(journal; stage="restart", tsk=0, pid="10", fault=false)
    @test journal["pids"]["10"]["restart"]["elapsed"] >= 0.0

    # Stop restart with fault
    journal["pids"]["10"]["tic"] = time()
    Schedulers.journal_stop!(journal; stage="restart", tsk=0, pid="10", fault=true)
    @test journal["pids"]["10"]["restart"]["faults"] == 1

    # reduce stage — same pid already exists, should not create new
    Schedulers.journal_start!(journal; stage="reduce", tsk=0, pid="20", hostname="host2")
    @test haskey(journal["pids"], "20")
end

@testset "journal_start! - reduced stage" begin
    journal = Schedulers.journal_init(1:3, tsks->nothing; reduce=true)
    Schedulers.journal_start!(journal; stage="tasks", tsk=1, pid=42, hostname="testhost")
    Schedulers.journal_start!(journal; stage="reduced", tsk=1, pid=42, hostname="testhost")
    @test journal["tasks"][1]["trials"][1]["reduced"] == false
    @test journal["tasks"][1]["trials"][1]["reducedat"] == ""

    Schedulers.journal_stop!(journal; stage="reduced", tsk=1, pid=42, fault=false)
    @test journal["tasks"][1]["trials"][1]["reduced"] == true
    @test journal["tasks"][1]["trials"][1]["reducedat"] != ""
end

@testset "ElasticLoop constructor" begin
    options = SchedulerOptions(;minworkers=0, maxworkers=5)

    # Without reduce — reduce channels should be closed
    eloop = Schedulers.ElasticLoop(Nothing, 1:10, options; isreduce=false)
    @test eloop.tsk_count == 10
    @test length(eloop.tsk_pool_todo) == 10
    @test isempty(eloop.tsk_pool_done)
    @test isempty(eloop.tsk_pool_reduced)
    @test !isopen(eloop.pid_channel_reduce_add)

    # With reduce — all channels open
    eloop_r = Schedulers.ElasticLoop(String, 1:5, options; isreduce=true)
    @test eloop_r.tsk_count == 5
    @test isopen(eloop_r.pid_channel_reduce_add)
end

@testset "ElasticLoop with usemaster" begin
    options = SchedulerOptions(;minworkers=0, maxworkers=5, usemaster=true)
    eloop = Schedulers.ElasticLoop(Nothing, 1:10, options; isreduce=false)
    @test eloop.epmap_use_master == true
    @test !(1 ∈ eloop.initialized_pids)  # usemaster=true → initialized_pids starts empty
end

@testset "total_tasks, pending_tasks, complete_tasks, reduced_tasks" begin
    options = SchedulerOptions(;minworkers=0, maxworkers=5)
    eloop = Schedulers.ElasticLoop(Nothing, 1:10, options; isreduce=false)

    @test total_tasks(eloop) == 10
    @test length(pending_tasks(eloop)) == 10
    @test isempty(complete_tasks(eloop))
    @test isempty(Schedulers.reduced_tasks(eloop))

    # Simulate completing tasks
    push!(eloop.tsk_pool_done, popfirst!(eloop.tsk_pool_todo))
    @test length(pending_tasks(eloop)) == 9
    @test length(complete_tasks(eloop)) == 1
end

@testset "reduce_checkpoints_is_dirty" begin
    options = SchedulerOptions(;minworkers=0, maxworkers=5)
    eloop = Schedulers.ElasticLoop(String, 1:10, options; isreduce=true)

    # No entries → not dirty
    @test Schedulers.reduce_checkpoints_is_dirty(eloop) == false

    # All false → not dirty
    eloop.reduce_checkpoints_is_dirty[1] = false
    eloop.reduce_checkpoints_is_dirty[2] = false
    @test Schedulers.reduce_checkpoints_is_dirty(eloop) == false

    # One true → dirty
    eloop.reduce_checkpoints_is_dirty[2] = true
    @test Schedulers.reduce_checkpoints_is_dirty(eloop) == true
end

@testset "trigger_reduction!" begin
    options = SchedulerOptions(;minworkers=0, maxworkers=5)
    eloop = Schedulers.ElasticLoop(String, 1:5, options; isreduce=true)

    @test !isready(eloop.reduce_trigger_channel)
    trigger_reduction!(eloop)
    @test isready(eloop.reduce_trigger_channel)
    val = take!(eloop.reduce_trigger_channel)
    @test val == true
end

@testset "SchedulerOptions constructor" begin
    # Default values
    opts = SchedulerOptions()
    @test opts.retries == 0
    @test opts.maxerrors == typemax(Int)
    @test opts.timeout_multiplier ≈ 5.0
    @test opts.skip_tasks_that_timeout == false
    @test opts.usemaster == false
    @test opts.reporttasks == true
    @test opts.keepcheckpoints == false
    @test opts.journalfile == ""

    # Int minworkers/maxworkers/quantum get wrapped in functions
    opts2 = SchedulerOptions(;minworkers=3, maxworkers=10, quantum=4)
    @test opts2.minworkers() == 3
    @test opts2.maxworkers() == 10
    @test opts2.quantum() == 4

    # Function minworkers/maxworkers/quantum stay as-is
    opts3 = SchedulerOptions(;minworkers=()->7, maxworkers=()->20)
    @test opts3.minworkers() == 7
    @test opts3.maxworkers() == 20

    # Single scratch gets wrapped in array
    opts4 = SchedulerOptions(;scratch="/tmp/test")
    @test opts4.scratch == ["/tmp/test"]

    # Array scratch stays as-is
    opts5 = SchedulerOptions(;scratch=["/tmp/a", "/tmp/b"])
    @test opts5.scratch == ["/tmp/a", "/tmp/b"]
end

@testset "Base.copy(SchedulerOptions)" begin
    opts = SchedulerOptions(;retries=3, maxerrors=50, scratch=["/tmp/a", "/tmp/b"])
    opts_copy = copy(opts)

    for field in fieldnames(SchedulerOptions)
        @test getfield(opts, field) == getfield(opts_copy, field)
    end

    # Verify scratch is a copy not the same reference
    push!(opts_copy.scratch, "/tmp/c")
    @test length(opts.scratch) == 2
    @test length(opts_copy.scratch) == 3
end

@testset "save_checkpoint and load_checkpoint internal" begin
    tmpfile = tempname()
    data = [10.0, 20.0, 30.0]
    fut = Future()
    put!(fut, data)
    Schedulers.save_checkpoint(Schedulers.default_save_checkpoint, fetch, tmpfile, fut, typeof(data))
    loaded = Schedulers.load_checkpoint(Schedulers.default_load_checkpoint, tmpfile, typeof(data))
    @test loaded ≈ data
    rm(tmpfile)
end

@testset "reduce internal" begin
    tmpdir = mktempdir()
    cp1 = joinpath(tmpdir, "cp1")
    cp2 = joinpath(tmpdir, "cp2")
    cp3 = joinpath(tmpdir, "cp3")

    serialize(cp1, [1.0, 2.0])
    serialize(cp2, [3.0, 4.0])

    T = Vector{Float64}
    Schedulers.reduce(
        Schedulers.default_reducer!,
        Schedulers.default_save_checkpoint,
        fetch,
        Schedulers.default_load_checkpoint,
        cp1, cp2, cp3, T
    )

    result = deserialize(cp3)
    @test result ≈ [4.0, 6.0]
    rm(tmpdir; recursive=true)
end

@testset "journal_start! with callback" begin
    journal = Schedulers.journal_init(1:3, tsks->nothing; reduce=false)
    callback_called = Ref(false)
    callback = tsk -> (callback_called[] = true)

    Schedulers.journal_start!(journal, callback; stage="tasks", tsk=1, pid=42, hostname="host")
    @test callback_called[]
end

@testset "journal_stop! with callback" begin
    journal = Schedulers.journal_init(1:3, tsks->nothing; reduce=false)
    callback_called = Ref(false)
    callback = tsk -> (callback_called[] = true)

    Schedulers.journal_start!(journal, callback; stage="tasks", tsk=1, pid=42, hostname="host")
    callback_called[] = false
    Schedulers.journal_stop!(journal, callback; stage="tasks", tsk=1, pid=42, fault=false)
    @test callback_called[]
end

@testset "journal_init with callback" begin
    callback_tasks = Ref{Any}(nothing)
    journal = Schedulers.journal_init(1:5, tsks -> (callback_tasks[] = collect(tsks)); reduce=false)
    @test callback_tasks[] == collect(1:5)
end

@testset "reduce_trigger function" begin
    options = SchedulerOptions(;minworkers=0, maxworkers=5)
    eloop = Schedulers.ElasticLoop(String, 1:5, options; isreduce=true)
    journal = Schedulers.journal_init(1:5, tsks->nothing; reduce=true)

    # No trigger → false
    result = Schedulers.reduce_trigger(eloop, journal, tsk->nothing)
    @test result == false
    @test eloop.is_reduce_triggered == false

    # Put trigger signal
    put!(eloop.reduce_trigger_channel, true)
    result = Schedulers.reduce_trigger(eloop, journal, tsk->nothing)
    @test result == true
    @test eloop.is_reduce_triggered == true
    @test eloop.checkpoints_are_flushed == false
end

@testset "reduce_trigger with user trigger that errors" begin
    options = SchedulerOptions(;
        minworkers=0, maxworkers=5,
        reduce_trigger = eloop -> error("user trigger error")
    )
    eloop = Schedulers.ElasticLoop(String, 1:5, options; isreduce=true)
    journal = Schedulers.journal_init(1:5, tsks->nothing; reduce=true)

    # Should not throw, just log
    result = Schedulers.reduce_trigger(eloop, journal, tsk->nothing)
    @test result == false
end

@testset "epmapreduce_fetch_apply" begin
    x = Future()
    put!(x, [0.0, 0.0, 0.0])
    f(localresult, tsk, a; b=1) = (localresult .+= a * b * tsk; nothing)
    Schedulers.epmapreduce_fetch_apply(x, Vector{Float64}, fetch, f, 5, 2; b=3)
    @test fetch(x) ≈ [30.0, 30.0, 30.0]
end

@testset "SchedulerOptions float coercion" begin
    opts = SchedulerOptions(;
        timeout_multiplier=3,
        timeout_function_multiplier=2,
        null_tsk_runtime_threshold=1,
        skip_tsk_tol_ratio=0,
        grace_period_ratio=0
    )
    @test opts.timeout_multiplier isa Float64
    @test opts.timeout_function_multiplier isa Float64
    @test opts.null_tsk_runtime_threshold isa Float64
    @test opts.skip_tsk_tol_ratio isa Float64
    @test opts.grace_period_ratio isa Float64
end

@testset "save_partial_reduction with empty checkpoints" begin
    options = SchedulerOptions(;minworkers=0, maxworkers=5)
    eloop = Schedulers.ElasticLoop(String, 1:5, options; isreduce=true)

    # Empty reduce_checkpoints → should warn, not error
    @test_logs (:warn, "reduction is empty, nothing to save.") Schedulers.save_partial_reduction(eloop)
end

@testset "save_partial_reduction with checkpoint" begin
    tmpdir = mktempdir()
    cp = joinpath(tmpdir, "test_checkpoint")
    serialize(cp, [1.0, 2.0, 3.0])

    saved_data = Ref{Any}(nothing)
    options = SchedulerOptions(;
        minworkers=0, maxworkers=5,
        save_partial_reduction = x -> (saved_data[] = x)
    )
    eloop = Schedulers.ElasticLoop(String, 1:5, options; isreduce=true)
    push!(eloop.reduce_checkpoints, "dummy")
    push!(eloop.reduce_checkpoints_snapshot, cp)

    Schedulers.save_partial_reduction(eloop)
    @test saved_data[] ≈ [1.0, 2.0, 3.0]
    @test isempty(eloop.reduce_checkpoints_snapshot)
    rm(tmpdir; recursive=true)
end

@testset "journal_start! rmcheckpoints stage" begin
    journal = Schedulers.journal_init(1:3, tsks->nothing; reduce=true)
    Schedulers.journal_start!(journal; stage="rmcheckpoints", tsk=1, pid=42, hostname="testhost")
    @test length(journal["rmcheckpoints"][1]["trials"]) == 1

    Schedulers.journal_stop!(journal; stage="rmcheckpoints", tsk=1, pid=42, fault=false)
    @test journal["rmcheckpoints"][1]["trials"][1]["status"] == "succeeded"
end

@testset "default callbacks" begin
    # epmap_default_preempt_channel_future returns nothing
    @test Schedulers.epmap_default_preempt_channel_future(1) === nothing

    # epmap_default_checkpoint_task returns nothing
    @test Schedulers.epmap_default_checkpoint_task(1) === nothing

    # epmap_default_restart_task returns nothing
    @test Schedulers.epmap_default_restart_task(1) === nothing

    # epmap_default_init returns nothing
    @test Schedulers.epmap_default_init(1) === nothing
end

@testset "logerror with VERSION branches" begin
    try
        error("version branch test")
    catch e
        io = IOBuffer()
        with_logger(ConsoleLogger(io, Logging.Debug)) do
            Schedulers.logerror(e, Logging.Debug)
        end
        s = String(take!(io))
        @test contains(s, "version branch test")
        @test contains(s, "error type:")
    end
end

@testset "reduce_trigger - full save_partial_reduction flow" begin
    tmpdir = mktempdir()
    cp = joinpath(tmpdir, "test_cp")
    serialize(cp, [10.0, 20.0])

    saved = Ref{Any}(nothing)
    options = SchedulerOptions(;
        minworkers=0, maxworkers=5,
        reduce_trigger = eloop -> nothing,
        save_partial_reduction = x -> (saved[] = x)
    )
    eloop = Schedulers.ElasticLoop(String, 1:5, options; isreduce=true)
    journal = Schedulers.journal_init(1:5, tsks->nothing; reduce=true)

    # Set up state for full save_partial_reduction flow:
    # is_reduce_triggered=true, checkpoints_are_flushed=true, not dirty, snapshot has 1 element
    eloop.is_reduce_triggered = true
    eloop.checkpoints_are_flushed = true
    push!(eloop.reduce_checkpoints, "dummy")
    push!(eloop.reduce_checkpoints_snapshot, cp)

    # Simulate some done tasks
    for i in 1:5
        push!(eloop.tsk_pool_done, popfirst!(eloop.tsk_pool_todo))
    end

    result = Schedulers.reduce_trigger(eloop, journal, tsk->nothing)
    @test result == false  # resets to false after partial reduction
    @test saved[] ≈ [10.0, 20.0]
    rm(tmpdir; recursive=true)
end

@testset "save_partial_reduction with failing user callback" begin
    tmpdir = mktempdir()
    cp = joinpath(tmpdir, "test_cp2")
    serialize(cp, [1.0])

    options = SchedulerOptions(;
        minworkers=0, maxworkers=5,
        save_partial_reduction = x -> error("user callback failed")
    )
    eloop = Schedulers.ElasticLoop(String, 1:5, options; isreduce=true)
    push!(eloop.reduce_checkpoints, "dummy")
    push!(eloop.reduce_checkpoints_snapshot, cp)

    # Should not throw, just log error
    @test_logs (:error, "problem running user-supplied save_partial_reduction") Schedulers.save_partial_reduction(eloop)
    rm(tmpdir; recursive=true)
end

end # top-level testset
