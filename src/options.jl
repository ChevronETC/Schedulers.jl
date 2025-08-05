"""
Configuration options for the scheduler.
"""

mutable struct SchedulerOptions{C}
    retries::Int
    maxerrors::Int
    timeout_multiplier::Float64
    timeout_function_multiplier::Float64
    null_tsk_runtime_threshold::Float64
    skip_tsk_tol_ratio::Float64
    grace_period_ratio::Float64
    skip_tasks_that_timeout::Bool
    minworkers::Function
    maxworkers::Function
    nworkers::Function
    usemaster::Bool
    quantum::Function
    addprocs::Function
    init::Function
    preempt_channel_future::Function
    checkpoint_task::Function
    restart_task::Function
    reporttasks::Bool
    keepcheckpoints::Bool
    journalfile::String
    journal_init_callback::Function
    journal_task_callback::Function
    # reduce specific:
    reducer!::Function
    zeros::Function
    scratch::Vector{C}
    id::String
    epmapreduce_fetch::Function
    save_checkpoint::Function
    load_checkpoint::Function
    rm_checkpoint::Function
    reduce_trigger::Function
    save_partial_reduction::Function
    gethostname::Function
end

function SchedulerOptions(;
    retries = 0,
    maxerrors = typemax(Int),
    timeout_multiplier = 5,
    timeout_function_multiplier = 5,
    null_tsk_runtime_threshold = 0.0,
    skip_tsk_tol_ratio = 0,
    grace_period_ratio = 0,
    skip_tasks_that_timeout = false,
    minworkers = Distributed.nworkers,
    maxworkers = Distributed.nworkers,
    nworkers = ()->Distributed.nprocs()-1,
    usemaster = false,
    quantum = ()->32,
    addprocs = Distributed.addprocs,
    init = epmap_default_init,
    preempt_channel_future = epmap_default_preempt_channel_future,
    checkpoint_task = epmap_default_checkpoint_task,
    restart_task = epmap_default_restart_task,
    reporttasks = true,
    keepcheckpoints = false,
    journalfile = "",
    journal_init_callback = tsks->nothing,
    journal_task_callback = tsk->nothing,
    # reduce specific
    reducer!::Function = default_reducer!,
    zeros = ()->nothing,
    scratch = ["/scratch"],
    id = randstring(6),
    epmapreduce_fetch = fetch,
    save_checkpoint = default_save_checkpoint,
    load_checkpoint = default_load_checkpoint,
    rm_checkpoint = default_rm_checkpoint,
    reduce_trigger = channel->nothing,
    save_partial_reduction = checkpoint->nothing,
    gethostname = gethostname,
)
    SchedulerOptions(
        retries,
        maxerrors,
        Float64(timeout_multiplier),
        Float64(timeout_function_multiplier),
        Float64(null_tsk_runtime_threshold),
        Float64(skip_tsk_tol_ratio),
        Float64(grace_period_ratio),
        skip_tasks_that_timeout,
        isa(minworkers, Function) ? minworkers : ()->minworkers,
        isa(maxworkers, Function) ? maxworkers : ()->maxworkers,
        nworkers,
        usemaster,
        isa(quantum, Function) ? quantum : ()->quantum,
        addprocs,
        init,
        preempt_channel_future,
        checkpoint_task,
        restart_task,
        reporttasks,
        keepcheckpoints,
        journalfile,
        journal_init_callback,
        journal_task_callback,
        reducer!,
        zeros,
        isa(scratch, AbstractArray) ? scratch : [scratch],
        id,
        epmapreduce_fetch,
        save_checkpoint,
        load_checkpoint,
        rm_checkpoint,
        reduce_trigger,
        save_partial_reduction,
        gethostname,
    )
end

function Base.copy(options::SchedulerOptions)
    SchedulerOptions(
        options.retries,
        options.maxerrors,
        options.timeout_multiplier,
        options.timeout_function_multiplier,
        options.null_tsk_runtime_threshold,
        options.skip_tsk_tol_ratio,
        options.grace_period_ratio,
        options.skip_tasks_that_timeout,
        options.minworkers,
        options.maxworkers,
        options.nworkers,
        options.usemaster,
        options.quantum,
        options.addprocs,
        options.init,
        options.preempt_channel_future,
        options.checkpoint_task,
        options.restart_task,
        options.reporttasks,
        options.keepcheckpoints,
        options.journalfile,
        options.journal_init_callback,
        options.journal_task_callback,
        options.reducer!,
        options.zeros,
        copy(options.scratch),
        options.id,
        options.epmapreduce_fetch,
        options.save_checkpoint,
        options.load_checkpoint,
        options.rm_checkpoint,
        options.reduce_trigger,
        options.save_partial_reduction,
        options.gethostname,
    )
end
