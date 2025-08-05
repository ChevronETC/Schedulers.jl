# Tests for epmap_collect functionality

@testset "epmap_collect basic functionality test" begin
    safe_addprocs(3)
    @everywhere using Distributed, Schedulers

    # Simple function that returns the square of the task
    @everywhere function square_task(tsk)
        return tsk^2
    end

    tasks = 1:10
    results, timed_out = epmap_collect(square_task, tasks)

    @test length(results) == 10
    @test length(timed_out) == 0
    @test results == [i^2 for i = 1:10]

    # Test with no tasks
    empty_results, empty_timed_out = epmap_collect(square_task, Int[])
    @test length(empty_results) == 0
    @test length(empty_timed_out) == 0

    rmprocs(workers())
end

@testset "epmap_collect with additional arguments" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers

    # Function that takes additional arguments
    @everywhere function multiply_task(tsk, multiplier, offset)
        return tsk * multiplier + offset
    end

    tasks = 1:5
    multiplier = 3
    offset = 10
    results, timed_out = epmap_collect(multiply_task, tasks, multiplier, offset)

    expected = [i * multiplier + offset for i in tasks]
    @test results == expected
    @test length(timed_out) == 0

    rmprocs(workers())
end

@testset "epmap_collect with keyword arguments" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers

    # Function that uses keyword arguments
    @everywhere function power_task(tsk; power = 2, add = 0)
        return tsk^power + add
    end

    tasks = 1:5
    results, timed_out = epmap_collect(power_task, tasks; power = 3, add = 1)

    expected = [i^3 + 1 for i in tasks]
    @test results == expected
    @test length(timed_out) == 0

    rmprocs(workers())
end

@testset "epmap_collect with different return types" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers

    # Function that returns different types
    @everywhere function mixed_return_task(tsk)
        if tsk % 2 == 0
            return "even_$tsk"
        else
            return tsk * 2
        end
    end

    tasks = 1:6
    results, timed_out = epmap_collect(mixed_return_task, tasks)

    @test results[1] == 2  # 1 * 2
    @test results[2] == "even_2"
    @test results[3] == 6  # 3 * 2
    @test results[4] == "even_4"
    @test results[5] == 10 # 5 * 2
    @test results[6] == "even_6"
    @test length(timed_out) == 0

    rmprocs(workers())
end

@testset "epmap_collect with array/complex returns" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers

    # Function that returns arrays
    @everywhere function array_task(tsk)
        return [tsk, tsk^2, tsk^3]
    end

    tasks = 1:4
    results, timed_out = epmap_collect(array_task, tasks)

    @test length(results) == 4
    @test results[1] == [1, 1, 1]
    @test results[2] == [2, 4, 8]
    @test results[3] == [3, 9, 27]
    @test results[4] == [4, 16, 64]
    @test length(timed_out) == 0

    rmprocs(workers())
end

@testset "epmap_collect with SchedulerOptions" begin
    safe_addprocs(4)
    @everywhere using Distributed, Schedulers

    @everywhere function delay_task(tsk)
        sleep(0.1)  # Small delay
        return tsk * 10
    end

    # Test with custom scheduler options
    options = SchedulerOptions(;
        reporttasks = false,  # Disable task reporting to reduce noise
        timeout_multiplier = 10,
        retries = 1,
    )

    tasks = 1:8
    results, timed_out = epmap_collect(options, delay_task, tasks)

    @test length(results) == 8
    @test results == [i * 10 for i = 1:8]
    @test length(timed_out) == 0

    rmprocs(workers())
end

# @testset "epmap_collect with task failures" begin
#     safe_addprocs(3)
#     @everywhere using Distributed, Schedulers

#     # Function that fails for certain tasks
#     @everywhere function failing_task(tsk)
#         if tsk == 5
#             error("Task $tsk failed intentionally")
#         end
#         return tsk * 2
#     end

#     # Configure to allow errors and retry
#     options = SchedulerOptions(;
#         maxerrors=10,  # Allow multiple errors
#         retries=0,     # Don't retry failed tasks
#         reporttasks=false
#     )

#     tasks = 1:8
#     results, timed_out = epmap_collect(options, failing_task, tasks)

#     # Check that non-failing tasks completed
#     @test results[1] == 2
#     @test results[2] == 4
#     @test results[3] == 6
#     @test results[4] == 8
#     @test results[5] === nothing  # Failed task should return nothing
#     @test results[6] == 12
#     @test results[7] == 14
#     @test results[8] == 16

#     rmprocs(workers())
# end

@testset "epmap_collect with timeout tasks" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers

    # Function that takes too long for certain tasks
    @everywhere function slow_task(tsk)
        if tsk == 3
            sleep(5)  # This should timeout
        else
            sleep(0.1)
        end
        return tsk * 3
    end

    # Configure with short timeout and skip timed out tasks
    options = SchedulerOptions(;
        timeout_function_multiplier = 2,  # Short timeout
        skip_tasks_that_timeout = true,
        null_tsk_runtime_threshold = 0.05,
        reporttasks = false,
    )

    tasks = 1:5
    results, timed_out = epmap_collect(options, slow_task, tasks)

    # Check that task 3 timed out
    @test length(timed_out) > 0
    @test 3 ∈ timed_out || results[3] === nothing  # Task 3 should either timeout or return nothing

    # Other tasks should complete
    @test results[1] == 3
    @test results[2] == 6
    # results[3] might be nothing if it timed out
    @test results[4] == 12
    @test results[5] == 15

    rmprocs(workers())
end

@testset "epmap_collect with growing cluster" begin
    safe_addprocs(1)  # Start with minimal workers
    @everywhere using Distributed, Schedulers

    @everywhere function compute_task(tsk)
        sleep(0.05)  # Small computational delay
        return tsk^2 + tsk
    end

    # Configure elastic scaling
    options = SchedulerOptions(;
        maxworkers = ()->min(8, nworkers()+3),  # Allow growth
        minworkers = ()->1,
        quantum = ()->2,  # Add 2 workers at a time
        reporttasks = false,
    )

    tasks = 1:20
    results, timed_out = epmap_collect(options, compute_task, tasks)

    @test length(results) == 20
    @test length(timed_out) == 0
    expected = [i^2 + i for i = 1:20]
    @test results == expected

    rmprocs(workers())
end

@testset "epmap_collect result ordering" begin
    safe_addprocs(3)
    @everywhere using Distributed, Schedulers

    # Function with variable execution time to test ordering
    @everywhere function variable_time_task(tsk)
        # Tasks with higher numbers sleep less (complete faster)
        sleep_time = 0.01 * (11 - tsk)
        sleep(sleep_time)
        return tsk * 100
    end

    tasks = 1:10
    results, timed_out = epmap_collect(variable_time_task, tasks)

    # Results should be in the same order as input tasks
    @test length(results) == 10
    @test length(timed_out) == 0
    expected = [i * 100 for i = 1:10]
    @test results == expected

    rmprocs(workers())
end

@testset "epmap_collect with custom task types" begin
    safe_addprocs(2)
    @everywhere using Distributed, Schedulers

    # Test with string tasks
    @everywhere function string_task(str_task)
        return uppercase(str_task) * "_processed"
    end

    string_tasks = ["hello", "world", "test", "julia"]
    results, timed_out = epmap_collect(string_task, string_tasks)

    @test length(results) == 4
    @test results[1] == "HELLO_processed"
    @test results[2] == "WORLD_processed"
    @test results[3] == "TEST_processed"
    @test results[4] == "JULIA_processed"
    @test length(timed_out) == 0

    rmprocs(workers())
end
