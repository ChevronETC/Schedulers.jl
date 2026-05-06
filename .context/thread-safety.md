# Thread Safety Audit — Schedulers.jl

## Status

`state.jl` contains lock-protected helpers using `eloop.state_lock::ReentrantLock`.
These helpers are comprehensive but **NOT wired in** — all code directly mutates shared fields.

## Shared Mutable State

### Task Management (UNPROTECTED)

| State | Access Pattern | Existing Helper |
|-------|---------------|-----------------|
| `tsk_pool_todo::Vector` | `popfirst!` from multiple `@async` workers | `pop_next_task!` |
| `tsk_pool_done::Vector` | `push!` from workers + `length` read by event loop | `mark_task_done!` |
| `tsk_pool_timed_out::Vector` | `push!` from workers | `mark_task_timed_out!` |
| `tsk_retried::Set{Int}` | `push!`/`in` from workers | (needs helper) |

### Checkpoint Collections (UNPROTECTED)

| State | Access Pattern | Existing Helper |
|-------|---------------|-----------------|
| `reduce_checkpoints::Vector` | Map pushes, reduce pops (different locks!) | `push_reduce_checkpoint!` |
| `reduce_checkpoints_snapshot::Vector` | Reduce trigger copies/modifies | `push_reduce_checkpoint_snapshot!` |
| `checkpoints::Dict{Int,C}` | Per-worker state in map phase | `set_worker_checkpoint!`, `flush_worker_checkpoint!` |
| `reduce_checkpoints_is_dirty::Dict` | Per-worker dirty flag (local lock only) | `set_reduce_dirty!`, `pop_reduce_dirty!` |

### Worker Tracking (UNPROTECTED)

| State | Access Pattern | Existing Helper |
|-------|---------------|-----------------|
| `initialized_pids::Set{Int}` | Event handlers read/modify | (needs helper) |
| `used_pids_map::Set{Int}` | Event handlers modify | (needs helper) |
| `used_pids_reduce::Set{Int}` | Event handlers modify | (needs helper) |
| `pid_failures::Dict{Int,Int}` | Exception handlers increment | `record_pid_failure!`, `get_pid_failures` |

### State Flags (UNPROTECTED)

| State | Access Pattern | Existing Helper |
|-------|---------------|-----------------|
| `interrupted::Bool` | Written by multiple places, read constantly | `set_interrupted!`, `is_interrupted` |
| `errored::Bool` | Set by exception handlers | `set_interrupted!(; errored=true)`, `is_errored` |
| `is_reduce_triggered::Bool` | Set by reduction.jl | (needs helper) |
| `grace_period_start_time::Float64` | Read by event loop, written by workers | `set_grace_period_start_time!` |

## Critical Race Conditions

### Race 1: Lost Task Execution (CRITICAL)

**Files:** `epmap.jl`, `epmapreduce.jl`

Multiple `@async` worker loops call `popfirst!(eloop.tsk_pool_todo)` concurrently.
Two workers can pop the same task, or one gets an `ArgumentError` on empty vector.

**Fix:** Replace with `pop_next_task!(eloop)` which holds `state_lock`.

### Race 2: Checkpoint Corruption (CRITICAL)

**File:** `epmapreduce.jl`

Map phase pushes to `reduce_checkpoints` with no lock.
Reduce phase pops from it using a local `lock(l)` that map doesn't share.

**Fix:** Both sides use `state_lock` via `push_reduce_checkpoint!` / locked pop.

### Race 3: Preemption Callback During User Function (CRITICAL)

**File:** `preemption.jl`

```
t = Threads.@spawn f(args...)          # worker thread pool
t_preempt = @async begin
    take!(preempt_channel)
    checkpoint_task(tsk)               # runs WHILE f() still executing!
    Base.throwto(t, InterruptException())
end
```

`checkpoint_task` saves state while `f()` is still mutating it on a real thread.
This is true parallelism — not just cooperative scheduling.

**Fix:** Interrupt `f()` first, wait for it to stop, THEN checkpoint:
```
take!(preempt_channel)
istaskdone(t) || Base.throwto(t, InterruptException())
wait(t)  # let f() finish handling the interrupt
checkpoint_task(tsk)
```

### Race 4: Event Loop vs Worker Mutations (HIGH)

**File:** `elastic_loop.jl`

Event loop reads `length(eloop.tsk_pool_done)` in `check_tasks_done!` while
worker `@async` tasks push to it. Stale reads cause incorrect scaling, grace
period miscalculation, or missed completion.

**Fix:** Use `tasks_done_count(eloop)` which holds `state_lock`.

### Race 5: Timing Vector (HIGH)

**File:** `timeout.jl`

`tsk_times` vector is read by Timer callback and appended by task completion.

**Fix:** Lock around push, or copy before reading in timer.

## Implementation Plan

Wire existing `state.jl` helpers into the codebase, file by file:

1. `epmap.jl` — `pop_next_task!`, `mark_task_done!`, `requeue_task!`, state flag reads
2. `epmapreduce.jl` — same + checkpoint helpers, unify reduce lock to `state_lock`
3. `errors.jl` — `record_pid_failure!`, `get_pid_failures`
4. `elastic_loop.jl` — `tasks_done_count`, pid set helpers, flag reads
5. `preemption.jl` — reverse checkpoint/interrupt order
6. `timeout.jl` — lock `tsk_times` access
