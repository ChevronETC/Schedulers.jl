# --- Event type hierarchy ---

abstract type SchedulerEvent end

# --- Worker lifecycle ---
abstract type WorkerEvent <: SchedulerEvent end

struct WorkerFreed <: WorkerEvent
    pid::Int
    is_bad::Bool
    phase::Symbol  # :map or :reduce
end

struct WorkerInitialized <: WorkerEvent
    pid::Int
end

struct WorkerInitFailed <: WorkerEvent
    pid::Int
end

# --- Phase lifecycle ---
abstract type PhaseEvent <: SchedulerEvent end

struct MapPhaseCompleted <: PhaseEvent end

struct MapPhaseFailed <: PhaseEvent
    exception::Union{Exception, Nothing}
end

struct ReducePhaseCompleted <: PhaseEvent end

struct ReducePhaseFailed <: PhaseEvent
    exception::Union{Exception, Nothing}
end

# --- Scaling ---
abstract type ScalingEvent <: SchedulerEvent end

struct ScaleTick <: ScalingEvent end

struct AddRmProcsCompleted <: ScalingEvent
    success::Bool
end

# --- Control ---
abstract type ControlEvent <: SchedulerEvent end

struct InterruptRequested <: ControlEvent end
struct ShutdownRequested <: ControlEvent end
