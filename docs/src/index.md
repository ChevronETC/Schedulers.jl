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

# Important note
Note that the error handling in the `epmapreduce!` method is, as of Schedulers 0.1.0, limited to the case where
a worker is removed, throwing a `ProcessExitedException`.  In future versions, we will work to expand the scope
of the error handling.