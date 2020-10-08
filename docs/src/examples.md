# Examples

## Parallel map
```julia
using Distributed

addprocs(5)
@everywhere using Distributed, Schedulers
@everywhere function foo(tsk)
    @info "sleeping for task $tsk on worker $(myid()) for 60 seconds"
    sleep(60)
end

epmap(foo, 1:20)
rmprocs(workers())
```

## Parallel map reduce
The parallel map reduce method `epmapreduce!` creates memory on each worker process for
storing a local reduction.  Upon exhaustion of the tasks, the local reductions
are reduced into a final reduction.  The memory allocated for each worker is
dictated by the first argument to `epmapreduce!` which is also the memory that
holds the final reduced result.
```julia
using Distributed

addprocs(5)
@everywhere using Distributed, Schedulers
@everywhere function foo(y, tsk)
    fetch(y)::Vector{Float64} .+= tsk
    @info "sleeping for task $tsk on worker $(myid()) for 60 seconds"
    sleep(60)
end
x = epmapreduce!(zeros(Float64,5), foo, 1:10) # x=[10,10,10,10,10]
```
Note that `y` in the above examples is a `Future` that contains the
partial reduction.

## Parallel map with elasticity
Both `epmap` and `epmapreduce` can accept `epmap_minworkers` and
`epmap_maxworkers` key-word arguments to control the elasticity.  In
addition the `epmap_quantum` and `epmap_addprocs` arguments are used to
control how workers are added.  `Distributed.rmprocs` is used to shrink
the number of workers.  For example, the number of workers will shrink
when the number of remaining tasks is less than the number of Julia
workers, or when a worker is deemed not usable due to fault handling.
```julia
using Distributed

addprocs(10)
@everywhere using Distributed, Schedulers
@everywhere function foo(tsk)
    @info "sleeping for task $tsk on worker $(myid()) for 60 seconds"
    sleep(60)
end

epmap(foo, 1:20; emap_minworkers=5, emap_maxworkers=15, emap_addprocs=addprocs)
rmprocs(workers())
```
Note that as epmap elastically adds workers, methods and packages that are defined
in `Main` will automatically be loaded on the added workers.
