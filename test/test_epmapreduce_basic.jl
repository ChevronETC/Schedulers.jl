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

@testset "epmapreduce basic tests" begin

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
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo6, 1:20, a; b=b)

    rmprocs(workers())
    for field ∈ fieldnames(SchedulerOptions)
        @test getfield(options_init, field) == getfield(options, field)
    end
    @test x ≈ sum(a*b*[1:20;]) * ones(10)
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
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo6b, 1:20, a; b=b)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:20;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
    rm(tmpdir; recursive=true, force=true)
end

@testset "pmapreduce, growing cluster test" begin
    safe_addprocs(5)
    @everywhere using Distributed, Schedulers, Random, Test
    s = randstring(6)
    @everywhere function foo12(x, tsk, a, b)
        x .+= a*b*tsk
        sleep(2)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    options = SchedulerOptions(; minworkers=5, maxworkers=11, scratch=tmpdir, addprocs=safe_addprocs)
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo12, 1:20, a, b)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:20;]) * ones(10)

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
        sleep(2)
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    _nworkers = 5

    local x
    options = SchedulerOptions(; maxworkers=()->_nworkers, scratch=tmpdir, nworkers=()->nprocs()-1)
    tsk = @async epmapreduce!(zeros(Float32,10), options, foo13, 1:20, a, b)

    sleep(8)
    _nworkers = 10
    sleep(8)
    @test nworkers() > 5

    x,tsks = fetch(tsk)

    rmprocs(workers())
    @test x ≈ sum(a*b*[1:20;]) * ones(10)

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
        nothing
    end

    a,b = 2,3

    tmpdir = mktempdir(;cleanup=false)

    my_zeros() = (y=zeros(Float32,10),z=zeros(Float32,10))
    x = my_zeros()

    options = SchedulerOptions(;maxworkers = 10, scratch = tmpdir, zeros = my_zeros, reducer! = (x,y)->(x.y .+= y.y; x.z .+= y.z; nothing))
    epmapreduce!(x, options, foo14, 1:20, a, b)

    rmprocs(workers())

    @test x.y ≈ sum([a*tsk for tsk in 1:20])*ones(Float32,10)
    @test x.z ≈ sum([b*tsk for tsk in 1:20])*ones(Float32,10)
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
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo15, 1:30, a; b=b)

    ncheckpoints = [length(readdir(tmpdir)) for tmpdir in tmpdirs]
    ncheckpoints_average = sum(ncheckpoints) / 3
    for i = 1:3
        @test ncheckpoints[i] > 0
        @test (ncheckpoints[i] - ncheckpoints_average) < .1*ncheckpoints_average
    end

    rmprocs(workers())
    @test x ≈ sum(a*b*[1:30;]) * ones(10)
    rm.(tmpdirs; recursive=true, force=true)
end

@testset "pmapreduce, partial reduction" begin
    using Distributed, Schedulers

    function foo16(r, i)
        r .+= i
        sleep(1)
        nothing
    end

    function my_reduce_trigger(eloop, ntasks)
        if length(complete_tasks(eloop)) - ntasks[] > 15
            trigger_reduction!(eloop)
            ntasks[] = length(complete_tasks(eloop))
        end
    end

    tmpdir = mktempdir(;cleanup=false)
    tmpfile = tempname()

    ntasks = Ref(0)
    options = SchedulerOptions(;maxworkers=10, scratch=tmpdir, reduce_trigger=eloop->my_reduce_trigger(eloop, ntasks), save_partial_reduction=input->write(tmpfile, input))
    r,tsks = epmapreduce!(zeros(10), options, foo16, 1:30)

    @test r ≈ sum([1:30;]) * ones(10)

    x = read!(tmpfile, zeros(10))
    @test x[1] >= sum([1:15;])
    for i in eachindex(x)
        @test x[i] ≈ x[1]
    end

    rm(tmpfile)
    rm(tmpdir; recursive=true, force=true)
end

@testset "epmapreduce! convenience form (minimal options)" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers
    @everywhere function foo_conv_reduce(x, tsk)
        x .+= tsk
        nothing
    end
    tmpdir = mktempdir(;cleanup=false)
    options = SchedulerOptions(;scratch=tmpdir)
    x, tsks = epmapreduce!(zeros(Float32, 5), options, foo_conv_reduce, 1:10)
    rmprocs(workers())
    @test x ≈ sum(1:10) * ones(Float32, 5)
    rm(tmpdir; recursive=true, force=true)
end

@testset "epmapreduce! with journalfile" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers
    @everywhere function foo_jf(x, tsk)
        x .+= tsk
        nothing
    end
    tmpdir = mktempdir(;cleanup=false)
    tmpfile = tempname() * ".json"
    options = SchedulerOptions(;scratch=tmpdir, journalfile=tmpfile, maxworkers=2)
    x, tsks = epmapreduce!(zeros(Float32, 5), options, foo_jf, 1:10)
    rmprocs(workers())
    @test isfile(tmpfile)
    content = read(tmpfile, String)
    @test contains(content, "tasks")
    @test contains(content, "done")
    @test x ≈ sum(1:10) * ones(Float32, 5)
    rm(tmpfile; force=true)
    rm(tmpdir; recursive=true, force=true)
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
    x,tsks = epmapreduce!(zeros(Float32,10), options, foo6, 1:20, a; b=b)
    rmprocs(workers())
    @test x ≈ sum(a*b*[1:20;]) * ones(10)
    @test mapreduce(file->startswith(file, "checkpoint"), +, ["x";readdir(tmpdir)]) == 0
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

end # @testset "epmapreduce basic tests"
