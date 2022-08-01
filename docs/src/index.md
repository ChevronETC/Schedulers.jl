# Schedulers.jl

Shedulers.jl provides elastic and fault tolerant parallel map (`epmap`) and parallel map reduce
methods (`epmapreduce!`).  The primary feature that distinguishes Schedulers parallel map method
from Julia's `Distributed.pmap` is elasticity where the cluster is permitted to dynamically grow/shrink.
The parallel map reduce method also aims for features of fault tolerance, dynamic load balancing and
elasticity.

This package can be used in conjunction with [AzSessions.jl](https://github.com/ChevronETC/AzSessions.jl),
[AzStorage.jl](https://github.com/ChevronETC/AzStorage.jl) and [AzManagers.jl](https://github.com/ChevronETC/AzManagers.jl)
to have reasonable parallel map and parallel map reduce methods that work with Azure cloud scale-sets.
The fault handling and elasticity allows us to, in particular, work with Azure cloud scale-sets that are using
Azure Spot instances.

# Implementation details
The current implementation of the parallel map reduce method uses shared storage (e.g. NFS, Azure Blob storage...)
to check-point local reductions.  This check-pointing is what allows for fault tolerance, and to some extent,
elasticity.  However, it is important to note that it comes at the cost of IO operations.  It would be useful
to investigate alternatives ideas that avoid IO such as resilient distributed data from Spark.

The map and the reduce in the `epmapreduce!` method are asynchronous.  The map is prioritized, but when the
number of tasks that need to be mapped over is less than the number of machines, the reduction over available
check-points can begin.  In certain cases this can help to hide the cost of the reduction by overlapping with
the map.  In addition, it can make better use of the cluster such that machines that would otherwise be idle
can be used for the reduction.