var documenterSearchIndex = {"docs":
[{"location":"examples/#Examples","page":"Examples","title":"Examples","text":"","category":"section"},{"location":"examples/#Parallel-map","page":"Examples","title":"Parallel map","text":"","category":"section"},{"location":"examples/","page":"Examples","title":"Examples","text":"using Distributed\n\naddprocs(5)\n@everywhere using Distributed, Schedulers\n@everywhere function foo(tsk)\n    @info \"sleeping for task $tsk on worker $(myid()) for 60 seconds\"\n    sleep(60)\nend\n\nepmap(foo, 1:20)\nrmprocs(workers())","category":"page"},{"location":"examples/#Parallel-map-reduce","page":"Examples","title":"Parallel map reduce","text":"","category":"section"},{"location":"examples/","page":"Examples","title":"Examples","text":"The parallel map reduce method epmapreduce! creates memory on each worker process for storing a local reduction.  Upon exhaustion of the tasks, the local reductions are reduced into a final reduction.  The memory allocated for each worker is dictated by the first argument to epmapreduce! which is also the memory that holds the final reduced result.","category":"page"},{"location":"examples/","page":"Examples","title":"Examples","text":"using Distributed\n\naddprocs(5)\n@everywhere using Distributed, Schedulers\n@everywhere function foo(y, tsk)\n    fetch(y)::Vector{Float64} .+= tsk\n    @info \"sleeping for task $tsk on worker $(myid()) for 60 seconds\"\n    sleep(60)\nend\nx = epmapreduce!(zeros(Float64,5), foo, 1:10) # x=[10,10,10,10,10]","category":"page"},{"location":"examples/","page":"Examples","title":"Examples","text":"Note that y in the above examples is a Future that contains the partial reduction.","category":"page"},{"location":"examples/#Parallel-map-reduce-with-structured-data","page":"Examples","title":"Parallel map reduce with structured data","text":"","category":"section"},{"location":"examples/","page":"Examples","title":"Examples","text":"By default the reduction assumes that the object being reduced is a Julia array. However, this can be customized to use an arbitrary object by specifying the epmap_zeros and epmap_reducer! key-work arguments.","category":"page"},{"location":"examples/","page":"Examples","title":"Examples","text":"using Distributed\n\naddprocs(5)\n@everywhere using Distributed, Schedulers\n@everywhere function foo(y, tsk)\n    a = (fetch(y).a)::Vector{Float64}\n    b = (fetch(y).b)::Vector{Float64}\n    a .+= tsk\n    b .+= 2*tsk\n    nothing\nend\n\n@everywhere my_zeros() = (a=zeros(Float64,5), b=zeros(Float64,5))\n@everywhere function my_reducer!(x,y)\n    x.a .+= y.a\n    x.b .+= y.b\n    nothing\nend\nx = epmapreduce!(my_zeros(), foo, 1:10;\n    epmap_zeros = my_zeros,\n    epmap_reducer! = my_reducer!)","category":"page"},{"location":"examples/#Parallel-map-with-elasticity","page":"Examples","title":"Parallel map with elasticity","text":"","category":"section"},{"location":"examples/","page":"Examples","title":"Examples","text":"Both epmap and epmapreduce can accept epmap_minworkers and epmap_maxworkers key-word arguments to control the elasticity.  In addition the epmap_quantum and epmap_addprocs arguments are used to control how workers are added.  Distributed.rmprocs is used to shrink the number of workers.  For example, the number of workers will shrink when the number of remaining tasks is less than the number of Julia workers, or when a worker is deemed not usable due to fault handling.","category":"page"},{"location":"examples/","page":"Examples","title":"Examples","text":"using Distributed\n\naddprocs(10)\n@everywhere using Distributed, Schedulers\n@everywhere function foo(tsk)\n    @info \"sleeping for task $tsk on worker $(myid()) for 60 seconds\"\n    sleep(60)\nend\n\nepmap(foo, 1:20; emap_minworkers=5, emap_maxworkers=15, emap_addprocs=addprocs)\nrmprocs(workers())","category":"page"},{"location":"examples/","page":"Examples","title":"Examples","text":"Note that as epmap elastically adds workers, methods and packages that are defined in Main will automatically be loaded on the added workers.","category":"page"},{"location":"reference/#Reference","page":"Reference","title":"Reference","text":"","category":"section"},{"location":"reference/","page":"Reference","title":"Reference","text":"Modules = [Schedulers]\nOrder   = [:function, :type]","category":"page"},{"location":"reference/#Schedulers.epmap-Tuple{Function,Any,Vararg{Any,N} where N}","page":"Reference","title":"Schedulers.epmap","text":"epmap(f, tasks, args...; pmap_kwargs..., f_kwargs...)\n\nwhere f is the map function, and tasks is an iterable collection of tasks.  The function f takes the positional arguments args, and the keyword arguments f_args.  The optional arguments pmap_kwargs are as follows.\n\npmap_kwargs\n\nepmap_retries=0 number of times to retry a task on a given machine before removing that machine from the cluster\nepmap_maxerrors=Inf the maximum number of errors before we give-up and exit\nepmap_minworkers=nworkers() the minimum number of workers to elastically shrink to\nepmap_maxworkers=nworkers() the maximum number of workers to elastically expand to\nepmap_usemaster=false assign tasks to the master process?\nepmap_nworkers=nworkers the number of machines currently provisioned for work[1]\nepmap_quantum=32 the maximum number of workers to elastically add at a time\nepmap_addprocs=n->addprocs(n) method for adding n processes (will depend on the cluster manager being used)\nepmap_init=pid->nothing after starting a worker, this method is run on that worker.\nepmap_preempted=()->false method for determining of a machine got pre-empted (removed on purpose)[2]\n\nNotes\n\n[1] The number of machines provisioined may be greater than the number of workers in the cluster since with some cluster managers, there may be a delay between the provisioining of a machine, and when it is added to the Julia cluster. [2] For example, on Azure Cloud a SPOT instance will be pre-empted if someone is willing to pay more for it\n\n\n\n\n\n","category":"method"},{"location":"reference/#Schedulers.epmapreduce!-Union{Tuple{N}, Tuple{T}, Tuple{T,Any,Any,Vararg{Any,N} where N}} where N where T","page":"Reference","title":"Schedulers.epmapreduce!","text":"epmapreduce!(result, f, tasks, args...; epmap_kwargs..., kwargs...) -> result\n\nwhere f is the map function, and tasks are an iterable set of tasks to map over.  The positional arguments args and the named arguments kwargs are passed to f which has the method signature: f(localresult, f, task, args; kwargs...).  localresult is a Future with an assoicated partial reduction.\n\nepmap_kwargs\n\nepmap_reducer! = default_reducer! the method used to reduce the result. The default is (x,y)->(x .+= y)\nepmap_zeros = ()->zeros(eltype(result), size(result)) the method used to initiaize partial reductions\nepmap_minworkers=nworkers() the minimum number of workers to elastically shrink to\nepmap_maxworkers=nworkers() the maximum number of workers to elastically expand to\nepmap_usemaster=false assign tasks to the master process?\nepmap_nworkers=nworkers the number of machines currently provisioned for work[1]\nepmap_quantum=32 the maximum number of workers to elastically add at a time\nepmap_addprocs=n->addprocs(n) method for adding n processes (will depend on the cluster manager being used)\nepmap_init=pid->nothing after starting a worker, this method is run on that worker.\nepmap_scratch=\"/scratch\" storage location accesible to all cluster machines (e.g NFS, Azure blobstore,...)\n\nNotes\n\n[1] The number of machines provisioined may be greater than the number of workers in the cluster since with some cluster managers, there may be a delay between the provisioining of a machine, and when it is added to the Julia cluster. [2] For example, on Azure Cloud a SPOT instance will be pre-empted if someone is willing to pay more for it\n\nExamples\n\nExample 1\n\nWith the assumption that /scratch is accesible from all workers:\n\nusing Distributed\naddprocs(2)\n@everywhere using Distributed, Schedulers\n@everywhere f(x, tsk) = (fetch(x)::Vector{Float32} .+= tsk; nothing)\nresult = epmapreduce!(zeros(Float32,10), f, 1:100)\nrmprocs(workers())\n\nExample 2\n\nUsing Azure blob storage:\n\nusing Distributed, AzStorage\ncontainer = AzContainer(\"scratch\"; storageaccount=\"mystorageaccount\")\nmkpath(container)\naddprocs(2)\n@everywhere using Distributed, Schedulers\n@everywhere f(x, tsk) = (fetch(x)::Vector{Float32} .+= tsk; nothing)\nresult = epmapreduce!(zeros(Float32,10), f, 1:100; epmap_scratch=container)\nrmprocs(workers())\n\n\n\n\n\n","category":"method"},{"location":"#Schedulers.jl","page":"Schedulers.jl","title":"Schedulers.jl","text":"","category":"section"},{"location":"","page":"Schedulers.jl","title":"Schedulers.jl","text":"Shedulers.jl provides elastic and fault tolerant parallel map (epmap) and parallel map reduce methods (epmapreduce!).  The primary feature that distinguishes Schedulers parallel map method from Julia's Distributed.pmap is elasticity where the cluster is permitted to dynamically grow/shrink. The parallel map reduce method also aims for features of fault tolerance, dynamic load balancing and elasticity.","category":"page"},{"location":"","page":"Schedulers.jl","title":"Schedulers.jl","text":"This package can be used in conjunction with AzSessions.jl, AzStorage.jl and AzManagers.jl to have reasonable parallel map and parallel map reduce methods that work with Azure cloud scale-sets. The fault handling and elasticity allows us to, in particular, work with Azure cloud scale-sets that are using Azure Spot instances.","category":"page"},{"location":"#Implementation-details","page":"Schedulers.jl","title":"Implementation details","text":"","category":"section"},{"location":"","page":"Schedulers.jl","title":"Schedulers.jl","text":"The current implementation of the parallel map reduce method uses shared storage (e.g. NFS, Azure Blob storage...) to check-point local reductions.  This check-pointing is what allows for fault tolerance, and to some extent, elasticity.  However, it is important to note that it comes at the cost of IO operations.  It would be useful to investigate alternatives ideas that avoid IO such as resilient distributed data from Spark.","category":"page"},{"location":"#Important-note","page":"Schedulers.jl","title":"Important note","text":"","category":"section"},{"location":"","page":"Schedulers.jl","title":"Schedulers.jl","text":"Note that the error handling in the epmapreduce! method is, as of Schedulers 0.1.0, limited to the case where a worker is removed, throwing a ProcessExitedException.  In future versions, we will work to expand the scope of the error handling.","category":"page"}]
}
