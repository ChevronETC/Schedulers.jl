# Each top-level @testset of testsets.jl runs in its own fresh process, in parallel
# (ParallelTestRunner.jl), and its output is printed as one block after the run.
# `julia --project=test test/runtests.jl --help` lists the options, e.g. `--list`,
# `--jobs=N`, or test names (prefixes, `!` to skip) to run only those.
using Schedulers
using ParallelTestRunner

# testsets.jl is shared setup (`using`, ENV, helpers) followed by top-level @testsets. Each
# @testset becomes one test named by its title, run after the setup in Main of a process of
# its own: the tests define functions on their workers with @everywhere and count every
# worker in the cluster.
const TESTSETS = joinpath(@__DIR__, "testsets.jl")
const setup = Expr(:toplevel)
testsuite = Dict{String,Expr}()
let line = nothing
    for ex in Meta.parseall(read(TESTSETS, String); filename=TESTSETS).args
        if ex isa LineNumberNode
            line = ex
        elseif Meta.isexpr(ex, :macrocall) && ex.args[1] === Symbol("@testset")
            testsuite[ex.args[3]] = quote
                cd(mktempdir())
                Core.eval(Main, $(QuoteNode(setup)))
                Core.eval(Main, $(QuoteNode(Expr(:toplevel, line, ex))))
            end
        else
            push!(setup.args, line, ex)
        end
    end
end

# A fresh process per test, with its own TMPDIR and working directory (above), which its workers
# inherit: tests write files to tempdir() and to the working directory under fixed names.
test_worker(name) = addworker(; env=["TMPDIR" => mktempdir()])

runtests(Schedulers, ARGS; testsuite, test_worker)
