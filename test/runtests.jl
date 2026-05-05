using Distributed, Logging, Random, Schedulers, Serialization, Test

ENV["JULIA_WORKER_TIMEOUT"] = "120"

# Run unit tests first (fast, no workers needed)
include("test_unit.jl")

# Integration test files to run in parallel as separate Julia processes.
# Each file has its own worker pool so they don't conflict.
test_files = [
    "test_epmap.jl",
    "test_epmapreduce_basic.jl",
    "test_epmapreduce_faults.jl",
]

julia_exe = joinpath(Sys.BINDIR, "julia")
test_dir = @__DIR__
coverage_flag = Base.JLOptions().code_coverage != 0

results = Channel{Pair{String,Bool}}(length(test_files))

@sync for tf in test_files
    Threads.@spawn begin
        cmd_args = coverage_flag ? ["--code-coverage=user"] : String[]
        cmd = `$julia_exe $cmd_args --project=test $(joinpath(test_dir, tf))`
        success = try
            run(cmd)
            true
        catch
            false
        end
        put!(results, tf => success)
    end
end
close(results)

failed = String[]
for (tf, ok) in results
    if ok
        @info "$tf: PASSED"
    else
        @error "$tf: FAILED"
        push!(failed, tf)
    end
end

if !isempty(failed)
    error("Test files failed: $(join(failed, ", "))")
end
