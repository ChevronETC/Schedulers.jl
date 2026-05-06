using Distributed, Logging, Schedulers, Test

ENV["JULIA_WORKER_TIMEOUT"] = "120"
ENV["JULIA_DEBUG"] = "Schedulers"

function safe_addprocs(n)
    try
        length(addprocs(n))
    catch e
        Schedulers.logerror(e)
        @warn "problem calling addprocs, nworkers=$(nworkers())"
    end
end

safe_addprocs(10)
@everywhere using Distributed, Schedulers

@testset "pmapreduce, partial reduction (debug)" begin
    function foo16(r, i)
        r .+= i
        sleep(1)
        nothing
    end

    function my_reduce_trigger(eloop, ntasks)
        n_complete = length(complete_tasks(eloop))
        if n_complete - ntasks[] > 15
            @info "TRIGGER: triggering reduction, n_complete=$n_complete, ntasks=$(ntasks[])"
            trigger_reduction!(eloop)
            ntasks[] = n_complete
        end
    end

    tmpdir = mktempdir(;cleanup=false)
    tmpfile = tempname()
    @info "tmpdir=$tmpdir, tmpfile=$tmpfile"

    ntasks = Ref(0)
    options = SchedulerOptions(;
        maxworkers=10,
        scratch=tmpdir,
        reduce_trigger=eloop->my_reduce_trigger(eloop, ntasks),
        save_partial_reduction=input->begin
            @info "save_partial_reduction called, writing $(length(input)) elements to $tmpfile"
            write(tmpfile, input)
        end
    )
    r,tsks = epmapreduce!(zeros(10), options, foo16, 1:30)

    rmprocs(workers())

    @info "result r = $r"
    @info "expected = $(sum([1:30;]) * ones(10))"
    @test r ≈ sum([1:30;]) * ones(10)

    @info "checking tmpfile=$tmpfile exists: $(isfile(tmpfile))"
    if isfile(tmpfile)
        x = read!(tmpfile, zeros(10))
        @info "partial reduction result x = $x"
        @test x[1] >= sum([1:15;])
        for i in eachindex(x)
            @test x[i] ≈ x[1]
        end
        rm(tmpfile)
    else
        @error "tmpfile does not exist — save_partial_reduction was never called successfully"
        # List what's in the scratch dir
        @info "scratch dir contents: $(readdir(tmpdir))"
        @test false  # force failure
    end

    rm(tmpdir; recursive=true, force=true)
end
