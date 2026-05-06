module Schedulers

using Dates, Distributed, JSON, Logging, Printf, Random, Serialization, Statistics

epmap_default_addprocs = n->addprocs(n)
epmap_default_preempt_channel_future = pid->nothing
epmap_default_checkpoint_task = pid->nothing
epmap_default_restart_task = pid->nothing
epmap_default_init = pid->nothing

include("logging.jl")
include("journal.jl")
include("types.jl")
include("errors.jl")
include("workers.jl")
include("preemption.jl")
include("timeout.jl")
include("checkpoints.jl")
include("reduction.jl")
include("elastic_loop.jl")
include("epmap.jl")
include("epmapreduce.jl")

export SchedulerOptions, epmap, epmapreduce!, trigger_reduction!, total_tasks, pending_tasks, complete_tasks

end
