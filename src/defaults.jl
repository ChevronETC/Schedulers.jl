"""
Default functions and configurations for the Schedulers module.
"""

const epmap_default_addprocs = n->addprocs(n)
const epmap_default_preempt_channel_future = pid->nothing
const epmap_default_checkpoint_task = pid->nothing
const epmap_default_restart_task = pid->nothing
const epmap_default_init = pid->nothing

default_reducer!(x, y) = (x .+= y; nothing)

default_save_checkpoint(checkpoint, localresult) = serialize(checkpoint, localresult)
default_load_checkpoint(checkpoint) = deserialize(checkpoint)
default_rm_checkpoint(checkpoint) = isfile(checkpoint) && rm(checkpoint)
