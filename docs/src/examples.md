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
    y .+= tsk
    @info "sleeping for task $tsk on worker $(myid()) for 60 seconds"
    sleep(60)
end
x = epmapreduce!(zeros(Float64,5), foo, 1:10) # x=[10,10,10,10,10]
```
Note that `y` contains a partial reduction for the tasks assigned to its Julia process.

## Parallel map reduce with structured data
By default the reduction assumes that the object being reduced is a Julia array.
However, this can be customized to use an arbitrary object by specifying the
`epmap_zeros` and `epmap_reducer!` key-work arguments.
```julia
using Distributed

addprocs(5)
@everywhere using Distributed, Schedulers
@everywhere function foo(y, tsk)
    a = y.a
    b = y.b
    a .+= tsk
    b .+= 2*tsk
    nothing
end

@everywhere my_zeros() = (a=zeros(Float64,5), b=zeros(Float64,5))
@everywhere function my_reducer!(x,y)
    x.a .+= y.a
    x.b .+= y.b
    nothing
end
options = SchedulerOptions(zeros = my_zeros, reducer! = my_reducer!)
x = epmapreduce!(my_zeros(), options, foo, 1:10)
```

## Parameterization
Both `epmap` and `epmapreduce!` can be controlled by a parameter-set
defined in `options::SchedulerOptions`, and that can be passed as the
first argument to `epmap` or the second argument to `epmapreduce`.  The parameters
that can be set are described in the `epmap` and `epmapreduce!` documentation.

### Parallel map with elasticity
As an example of parameterizing `epmap`, we consider allow the compute cluster
to grow and shrink during the map.  `options::SchedulerOptions` include
`options.minworkers` and `options.maxworkers` to control elasticity.
```julia
using Distributed

addprocs(10)
@everywhere using Distributed, Schedulers
@everywhere function foo(tsk)
    @info "sleeping for task $tsk on worker $(myid()) for 60 seconds"
    sleep(60)
end

epmap(SchedulerOptions(;minworkers=5, maxworkers=15), foo, 1:20)
rmprocs(workers())
```

In addition `options.quantum` and `options.addprocs` can be
used to control how workers are added.  `Distributed.rmprocs` is used to shrink
the number of workers.  Further, note that the `epmap_nworkers` method
allows you to specify a method that returns the current number of allocated
workers.  This is useful for cloud computing where there is latency associated
with provisioning.  See for example the `nworkers_provisioned` method in
[AzManagers.jl](https://github.com/ChevronETC/AzManagers.jl).  As an example of
elasticity, the number of workers will shrink when the number of remaining tasks
is less than the number of Julia workers, or when a worker is deemed not usable
due to fault handling. Note that as epmap elastically adds workers, methods 
and packages that are defined in `Main` will automatically be loaded on the added workers.
