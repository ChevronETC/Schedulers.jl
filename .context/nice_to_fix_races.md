# Nice-to-Fix Races (Low/Medium Severity)

These are theoretically imprecise under cooperative scheduling but not practically dangerous today. They would matter if Julia ever adds preemptive Task scheduling.

## #2 — `is_reduce_triggered` reads without lock (MEDIUM)

Map worker @async tasks read `epmap_eloop.is_reduce_triggered` and `epmap_eloop.checkpoints_are_flushed` outside any lock (epmapreduce.jl ~line 209). The event loop writes these in `reduce_trigger` and `try_assign_worker!`. A stale read causes at most one extra loop iteration — never corruption.

**Files**: `src/epmapreduce.jl`, `src/elastic_loop.jl`, `src/reduction.jl`

**Fix**: Create locked helpers `is_reduce_triggered(eloop)` and `is_checkpoints_flushed(eloop)`, replace all direct reads.

## #3 — Compound condition on `is_reduce_triggered + checkpoints_are_flushed` (MEDIUM)

In `try_assign_worker!` (elastic_loop.jl ~line 229-238), the compound condition reads `is_reduce_triggered` then `checkpoints_are_flushed` as two separate field accesses. If a task switch happens between them, the pair could be inconsistent.

**Fix**: Read both under a single `lock(state_lock)` block.

## #4 — `tsk_pool_done` iteration without lock (LOW)

`check_reduce_done!` (elastic_loop.jl ~line 156) and `reduce_trigger` (reduction.jl ~line 62) iterate `eloop.tsk_pool_done` without holding `state_lock`. A concurrent `push!` from a map worker (via `mark_task_done!`) during a yield inside the loop body could cause the iterator to see a partially-updated vector. In practice, this only affects journaling — missing one task in a journal batch is harmless since `tsk_pool_reduced = copy(tsk_pool_done)` catches up.

**Fix**: Copy the vector under lock before iterating: `done_snapshot = lock(state_lock) do; copy(tsk_pool_done); end`.

## #5 — `tsk_pool_reduced` writes without lock (LOW)

`eloop.tsk_pool_reduced = copy(eloop.tsk_pool_done)` in `check_reduce_done!` and `reduce_trigger` is a field reassignment (not mutation). Only races if a user-supplied `reduce_trigger` callback yields while reading `reduced_tasks(eloop)`.

**Fix**: Wrap in `lock(state_lock)`.

## #6 — `empty!(epmap_eloop.checkpoints)` after @sync (LOW)

In `epmapreduce_map` (~line 361), `empty!(epmap_eloop.checkpoints)` runs after the `@sync` block completes (all map workers exited). Nothing else writes `checkpoints` at that point, so this is **not actually racy** — but wrapping in lock would be defensive.

## #7 — `used_pids_map/reduce` in `scale_workers!` @async (LOW)

The `filter(pid->pid ∉ eloop.used_pids_map && pid ∉ eloop.used_pids_reduce, workers())` inside the `@async` block in `scale_workers!` reads these sets without lock. However, all mutations to `used_pids_map/reduce` happen in event handlers (`handle_event!`, `try_assign_worker!`) which run in the same event loop task. The only concurrent writers are map/reduce @async workers, but those only `put!` events — they don't touch these sets. So this is **not actually racy** under the current architecture.

**Fix (defensive)**: Snapshot the sets under lock before spawning the @async.
