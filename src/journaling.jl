"""
Journaling functionality for tracking task execution and progress.
"""

function journal_init(tsks, epmap_journal_init_callback; reduce)
    journal =
        reduce ?
        Dict{String,Any}(
            "tasks"=>Dict(),
            "checkpoints"=>Dict(),
            "rmcheckpoints"=>Dict(),
            "pids"=>Dict(),
        ) : Dict{String,Any}("tasks"=>Dict())
    journal["start"] = now_formatted()
    for tsk in tsks
        journal["tasks"][tsk] = Dict("id"=>"$tsk", "trials"=>Dict[])
        if reduce
            journal["checkpoints"][tsk] = Dict("id"=>"$tsk", "trials"=>Dict[])
            journal["rmcheckpoints"][tsk] = Dict("id"=>"$tsk", "trials"=>Dict[])
        end
    end
    epmap_journal_init_callback(tsks)
    journal
end

function journal_final(journal)
    if haskey(journal, "pids")
        for pid in keys(journal["pids"])
            pop!(journal["pids"][pid], "tic")
        end
    end
    journal["done"] = now_formatted()
end

function journal_write(journal, filename)
    if !isempty(filename)
        write(filename, json(journal, 2))
    end
end

function journal_start!(
    journal,
    epmap_journal_task_callback = tsk->nothing;
    stage,
    tsk,
    pid,
    hostname,
)
    if stage ∈ ("tasks", "checkpoints", "rmcheckpoints")
        push!(
            journal[stage][tsk]["trials"],
            Dict("pid" => pid, "hostname" => hostname, "start" => now_formatted()),
        )
    elseif stage ∈ ("restart", "reduce") && pid ∉ keys(journal["pids"])
        journal["pids"][pid] = Dict(
            "restart" => Dict("elapsed" => 0.0, "faults" => 0),
            "reduce" => Dict("elapsed" => 0.0, "faults" => 0),
            "hostname" => hostname,
            "tic" => time(),
        )
    end

    if stage == "reduced"
        journal["tasks"][tsk]["trials"][end]["reduced"] = false
        journal["tasks"][tsk]["trials"][end]["reducedat"] = ""
    end

    if stage ∈ ("tasks", "reduced")
        epmap_journal_task_callback(journal["tasks"][tsk])
    end
end

function journal_stop!(
    journal,
    epmap_journal_task_callback = tsk->nothing;
    stage,
    tsk,
    pid,
    fault,
)
    if stage ∈ ("tasks", "checkpoints", "rmcheckpoints")
        journal[stage][tsk]["trials"][end]["status"] = fault ? "failed" : "succeeded"
        journal[stage][tsk]["trials"][end]["stop"] = now_formatted()
    elseif stage == "reduced"
        journal["tasks"][tsk]["trials"][end]["reduced"] = true
        journal["tasks"][tsk]["trials"][end]["reducedat"] = now_formatted()
    elseif stage ∈ ("restart", "reduce")
        journal["pids"][pid][stage]["elapsed"] += time() - journal["pids"][pid]["tic"]
        if fault
            journal["pids"][pid][stage]["faults"] += 1
        end
    end

    if stage ∈ ("tasks", "reduced")
        epmap_journal_task_callback(journal["tasks"][tsk])
    end
end
