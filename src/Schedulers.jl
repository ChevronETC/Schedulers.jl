"""
# Schedulers.jl

A Julia package for elastic parallel map-reduce computations with fault tolerance
and dynamic resource management.
"""
module Schedulers

using Dates, Distributed, JSON, Logging, Printf, Random, Serialization, Statistics

# Include submodules
include("utils.jl")
include("exceptions.jl")
include("defaults.jl")
include("journaling.jl")
include("workers.jl")
include("elastic_loop.jl")
include("timeouts.jl")
include("checkpoints.jl")
include("options.jl")
include("epmap.jl")
include("epmapreduce.jl")

# Re-export public API
export SchedulerOptions,
    epmap,
    epmapreduce!,
    trigger_reduction!,
    total_tasks,
    pending_tasks,
    complete_tasks,
    reduced_tasks


end
