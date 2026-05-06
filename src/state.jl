# --- Locked state access helpers ---
# These functions acquire eloop.state_lock briefly, mutate, and release.
# RULE: Never call remotecall, sleep, take!, wait, or put! while holding the lock.

function pop_next_task!(eloop)
    lock(eloop.state_lock) do
        isempty(eloop.tsk_pool_todo) && return nothing
        popfirst!(eloop.tsk_pool_todo)
    end
end

function mark_task_done!(eloop, tsk)
    lock(eloop.state_lock) do
        push!(eloop.tsk_pool_done, tsk)
    end
end

function mark_task_timed_out!(eloop, tsk)
    lock(eloop.state_lock) do
        push!(eloop.tsk_pool_done, tsk)
        push!(eloop.tsk_pool_timed_out, tsk)
    end
end

function requeue_task!(eloop, tsk)
    lock(eloop.state_lock) do
        push!(eloop.tsk_pool_todo, tsk)
    end
end

function tasks_remaining(eloop)
    lock(eloop.state_lock) do
        length(eloop.tsk_pool_todo)
    end
end

function tasks_done_count(eloop)
    lock(eloop.state_lock) do
        length(eloop.tsk_pool_done)
    end
end

function set_interrupted!(eloop; errored=false)
    lock(eloop.state_lock) do
        eloop.interrupted = true
        errored && (eloop.errored = true)
    end
end

function is_interrupted(eloop)
    lock(eloop.state_lock) do
        eloop.interrupted
    end
end

function is_errored(eloop)
    lock(eloop.state_lock) do
        eloop.errored
    end
end

function record_pid_failure!(eloop, pid)
    lock(eloop.state_lock) do
        eloop.pid_failures[pid] = get(eloop.pid_failures, pid, 0) + 1
    end
end

function get_pid_failures(eloop, pid)
    lock(eloop.state_lock) do
        get(eloop.pid_failures, pid, 0)
    end
end

function init_pid_failures!(eloop, pid)
    lock(eloop.state_lock) do
        eloop.pid_failures[pid] = 0
    end
end

function remove_pid_failures!(eloop, pid)
    lock(eloop.state_lock) do
        haskey(eloop.pid_failures, pid) && pop!(eloop.pid_failures, pid)
    end
end

# --- Checkpoint state helpers ---

function push_reduce_checkpoint!(eloop, checkpoint)
    lock(eloop.state_lock) do
        push!(eloop.reduce_checkpoints, checkpoint)
    end
end

function push_reduce_checkpoints!(eloop, checkpoints...)
    lock(eloop.state_lock) do
        push!(eloop.reduce_checkpoints, checkpoints...)
    end
end

function push_reduce_checkpoint_snapshot!(eloop, checkpoint)
    lock(eloop.state_lock) do
        push!(eloop.reduce_checkpoints_snapshot, checkpoint)
    end
end

function push_reduce_checkpoint_and_snapshot!(eloop, checkpoint)
    lock(eloop.state_lock) do
        push!(eloop.reduce_checkpoints, checkpoint)
        eloop.is_reduce_triggered && push!(eloop.reduce_checkpoints_snapshot, checkpoint)
    end
end

function set_worker_checkpoint!(eloop, pid, checkpoint)
    lock(eloop.state_lock) do
        eloop.checkpoints[pid] = checkpoint
    end
end

function get_worker_checkpoint(eloop, pid)
    lock(eloop.state_lock) do
        get(eloop.checkpoints, pid, nothing)
    end
end

function flush_worker_checkpoint!(eloop, pid)
    lock(eloop.state_lock) do
        checkpoint = get(eloop.checkpoints, pid, nothing)
        if checkpoint !== nothing
            push!(eloop.reduce_checkpoints, checkpoint)
        end
        delete!(eloop.checkpoints, pid)
        checkpoint
    end
end

function set_reduce_dirty!(eloop, pid, value)
    lock(eloop.state_lock) do
        eloop.reduce_checkpoints_is_dirty[pid] = value
    end
end

function pop_reduce_dirty!(eloop, pid)
    lock(eloop.state_lock) do
        pop!(eloop.reduce_checkpoints_is_dirty, pid)
    end
end

function get_grace_period_start_time(eloop)
    lock(eloop.state_lock) do
        eloop.grace_period_start_time
    end
end

function set_grace_period_start_time!(eloop, t)
    lock(eloop.state_lock) do
        eloop.grace_period_start_time = t
    end
end

function set_errored!(eloop)
    lock(eloop.state_lock) do
        eloop.errored = true
    end
end

function tsk_retried_add!(eloop, tsk)
    lock(eloop.state_lock) do
        push!(eloop.tsk_retried, tsk)
    end
end

function tsk_was_retried(eloop, tsk)
    lock(eloop.state_lock) do
        tsk in eloop.tsk_retried
    end
end

function total_pid_failures(eloop)
    lock(eloop.state_lock) do
        sum(values(eloop.pid_failures); init=0)
    end
end
