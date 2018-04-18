# Schedulers.jl

Schedulers.jl provides elastic and fault tolerant parallel map and parallel map reduce methods.
The primary feature that distinguishes Schedulers parallel map method from Julia's `Distributed.pmap`
is elasticity where the cluster is permitted to dynamically grow/shrink. The parallel map reduce
method also aims for features of fault tolerance, dynamic load balancing and elasticity.

| **Documentation** | **Action Statuses** |
|:---:|:---:|
| [![][docs-dev-img]][docs-dev-url] [![][docs-stable-img]][docs-stable-url] | [![][doc-build-status-img]][doc-build-status-url] [![][build-status-img]][build-status-url] [![][code-coverage-img]][code-coverage-results] |


[docs-dev-img]: https://img.shields.io/badge/docs-dev-blue.svg
[docs-dev-url]: https://chevronetc.github.io/Schedulers.jl/dev/

[docs-stable-img]: https://img.shields.io/badge/docs-stable-blue.svg
[docs-stable-url]: https://ChevronETC.github.io/Schedulers.jl/stable

[doc-build-status-img]: https://github.com/ChevronETC/Schedulers.jl/workflows/Documentation/badge.svg
[doc-build-status-url]: https://github.com/ChevronETC/Schedulers.jl/actions?query=workflow%3ADocumentation

[build-status-img]: https://github.com/ChevronETC/Schedulers.jl/workflows/Tests/badge.svg
[build-status-url]: https://github.com/ChevronETC/Schedulers.jl/actions?query=workflow%3A"Tests"

[code-coverage-img]: https://codecov.io/gh/ChevronETC/Schedulers.jl/branch/master/graph/badge.svg
[code-coverage-results]: https://codecov.io/gh/ChevronETC/Schedulers.jl

Elastic and fault tolerant `map` and `mapreduce` methods for traditional HPC and cloud computing.
