const no_value = "--no-value-sentinel--"


"""
    Worker

A `Worker` represents a worker endpoint in the distributed cluster. It accepts instructions
from the scheduler, fetches dependencies, executes compuations, stores data, and
communicates state to the scheduler.

# Fields
- `status::Symbol`: status of this worker

- `address::Address`:: ip address and port that this worker is listening on
- `listener::Base.TCPServer`: tcp server that listens for incoming connections

- `scheduler_address::Address`: the dask-distributed scheduler ip address and port info
- `batched_stream::Nullable{BatchedSend}`: batched stream for communication with scheduler
- `scheduler::Rpc`: manager for discrete send/receive open connections to the scheduler
- `connection_pool::ConnectionPool`: manages connections to peers

- `handlers::Dict{String, Function}`: handlers for operations requested by open connections
- `compute_stream_handlers::Dict{String, Function}`: handlers for compute stream operations

- `transitions::Dict{Tuple, Function}`: valid transitions that a task can make
- `data_needed::Deque{String}`: keys whose data we still lack
- `ready::PriorityQueue{String, Tuple, Base.Order.ForwardOrdering}`: keys ready to run
- `data::Dict{String, Any}`: maps keys to the results of function calls (actual values)
- `tasks::Dict{String, Tuple}`: maps keys to the function, args, and kwargs of a task
- `task_state::Dict{String, Symbol}`: maps keys tot heir state: (waiting, executing, memory)
- `priorities::Dict{String, Tuple}`: run time order priority of a key given by the scheduler
- `priority_counter::Int`: used to prioritize tasks by their order of arrival

- `dep_transitions::Dict{Tuple, Function}`: valid transitions that a dependency can make
- `dep_state::Dict{String, Symbol}`: maps dependencies with their state
    (waiting, flight, memory)
- `dependencies::Dict{String, Set}`: maps a key to the data it needs to run
- `dependents::Dict{String, Set}`: maps a dependency to the keys that use it
- `waiting_for_data::Dict{String, Set}`: maps a key to the data it needs that we don't have
- `pending_data_per_worker::DefaultDict{String, Deque}`: data per worker that we want
- `who_has::Dict{String, Set}`: maps keys to the workers believed to have their data
- `has_what::DefaultDict{String, Set{String}}`: maps workers to the data they have
- `in_flight_workers::Dict{String, Set}`: workers from which we are getting data from
- `missing_dep_flight::Set{String}`: missing dependencies
"""
type Worker <: Server
    status::Symbol

    # Server
    address::Address
    listener::Base.TCPServer

    # Communication management
    scheduler_address::Address
    batched_stream::Nullable{BatchedSend}
    scheduler::Rpc
    connection_pool::ConnectionPool

    # Handlers
    handlers::Dict{String, Function}
    compute_stream_handlers::Dict{String, Function}

    # Task state management
    transitions::Dict{Tuple{Symbol, Symbol}, Function}
    data_needed::Deque{String}
    ready::PriorityQueue{String, Tuple{Int, Int, Int}, Base.Order.ForwardOrdering}
    data::Dict{String, Any}
    tasks::Dict{String, Tuple{Base.Callable, Tuple, Vector{Any}, Nullable{DeferredFuture}}}

    task_state::Dict{String, Symbol}
    priorities::Dict{String, Tuple{Int, Int, Int}}
    priority_counter::Int

    # Dependency management
    dep_transitions::Dict{Tuple{Symbol, Symbol}, Function}
    dep_state::Dict{String, Symbol}
    dependencies::Dict{String, Set{String}}
    dependents::Dict{String, Set{String}}
    waiting_for_data::Dict{String, Set{String}}
    pending_data_per_worker::DefaultDict{String, Deque{String}}
    who_has::Dict{String, Set{String}}
    has_what::DefaultDict{String, Set{String}}
    in_flight_workers::Dict{String, Set{String}}
    missing_dep_flight::Set{String}
end

"""
    Worker(scheduler_address::String="127.0.0.1:8786")

Create a `Worker` that listens on a random port between 1024 and 9000 for incoming
messages. By default if the scheduler's address is not provided it assumes that the
dask-scheduler is being run on the same machine and on the default port 8786.

**NOTE**: Worker's must be started in the same julia cluster as the `DaskExecutor` (and it's
`Client`).

## Usage

```julia
Worker()  # The dask-scheduler is being run on the same machine on its default port 8786.
```

or also

```julia
Worker("\$(getipaddr()):8786") # Scheduler is running on the same machine
```

If running the dask-scheduler on a different machine or port:

* First start the `dask-scheduler` and inspect its startup logs:

```
\$ dask-scheduler
distributed.scheduler - INFO - -----------------------------------------------
distributed.scheduler - INFO -   Scheduler at:   tcp://127.0.0.1:8786
distributed.scheduler - INFO - etc.
distributed.scheduler - INFO - -----------------------------------------------
```

* Then start workers with it's printed address:

```julia
Worker("tcp://127.0.0.1:8786")
```

No further actions are needed directly on the Worker's themselves as they will communicate
with the `dask-scheduler` independently. New `Worker`s can be added/removed at any time
during execution. There usually should be at least one `Worker` to run computations.

## Cleanup

To explicitly shutdown a worker and delete it's information use:

```julia
worker = Worker()
shutdown([worker.address])
```

It is more effective to explicitly reset the [`DaskExecutor`](@ref) or shutdown a
[`Client`](@ref) rather than a `Worker` because the dask-scheduler will automatically
re-schedule the lost computations on other `Workers` if it thinks that a [`Client`](@ref)
still needs the lost data.

`Worker`'s are lost if they were spawned on a julia process that exits or is removed
via `rmprocs` from the julia cluster. It is cleaner but not necessary to explicity call
`shutdown` if planning to remove a `Worker`.
"""
function Worker(scheduler_address::String="127.0.0.1:8786")
    scheduler_address = Address(scheduler_address)
    port, listener = listenany(rand(1024:9000))
    worker_address = Address(getipaddr(), port)

    # This is the minimal set of handlers needed
    # https://github.com/JuliaParallel/Dagger.jl/issues/53
    handlers = Dict{String, Function}(
        "get_data" => get_data,
        "gather" => gather,
        "update_data" => update_data,
        "delete_data" => delete_data,
        "terminate" => terminate,
        "keys" => get_keys,
    )
    compute_stream_handlers = Dict{String, Function}(
        "compute-task" => add_task,
        "release-task" => release_key,
        "delete-data" => delete_data,
    )
    transitions = Dict{Tuple{Symbol, Symbol}, Function}(
        (:waiting, :ready) => transition_waiting_ready,
        (:waiting, :memory) => transition_waiting_done,
        (:ready, :executing) => transition_ready_executing,
        (:ready, :memory) => transition_ready_done,
        (:executing, :memory) => transition_executing_done,
    )
    dep_transitions = Dict{Tuple{Symbol, Symbol}, Function}(
        (:waiting, :flight) => transition_dep_waiting_flight,
        (:waiting, :memory) => transition_dep_waiting_memory,
        (:flight, :waiting) => transition_dep_flight_waiting,
        (:flight, :memory) => transition_dep_flight_memory,
    )
    worker = Worker(
        :starting,  # status

        worker_address,
        listener,

        scheduler_address,
        Nullable(), #  batched_stream
        Rpc(scheduler_address),  # scheduler
        ConnectionPool(),  # connection_pool

        handlers,
        compute_stream_handlers,

        transitions,
        Deque{String}(),  # data_needed
        PriorityQueue(String, Tuple{Int, Int, Int}, Base.Order.ForwardOrdering()),  # ready
        Dict{String, Any}(),  # data
        Dict{String, Tuple{Base.Callable, Tuple, Vector{Any}, Nullable{DeferredFuture}}}(),  # tasks
        Dict{String, String}(),  #task_state
        Dict{String, Tuple{Int, Int, Int}}(),  # priorities
        0,  # priority_counter

        dep_transitions,
        Dict{String, String}(),  # dep_state
        Dict{String, Set{String}}(),  # dependencies
        Dict{String, Set{String}}(),  # dependents
        Dict{String, Set{String}}(),  # waiting_for_data
        DefaultDict{String, Deque{String}}(Deque{String}),  # pending_data_per_worker
        Dict{String, Set{String}}(),  # who_has
        DefaultDict{String, Set{String}}(Set{String}),  # has_what
        Dict{String, Set{String}}(),  # in_flight_workers
        Set{String}(),  # missing_dep_flight
    )

    start(worker)
    return worker
end

"""
    shutdown(workers::Vector{Address})

Connect to and terminate all workers in `workers`.
"""
function shutdown(workers::Vector{Address})
    closed = Vector{Address}()

    for worker_address in workers
        clientside = connect(worker_address)

        response = send_recv(
            clientside,
            Dict("op" => "terminate", "reply" => "true")
        )
        if response == "OK"
            push!(closed, worker_address)
        else
            warn(logger, "Error closing worker \"$worker_address\": $response")
        end
    end
    notice(logger, "Shutdown $(length(closed)) worker(s) at: $closed")
end

"""
    show(io::IO, worker::Worker)

Print a representation of the worker and it's state.
"""
function Base.show(io::IO, worker::Worker)
    @printf(
        io,
        "<%s: %s, %s, stored: %d, ready: %d, waiting: %d>",
        typeof(worker).name.name,
        worker.address,
        worker.status,
        length(worker.data),
        length(worker.ready),
        length(worker.waiting_for_data),
    )
end

##############################         ADMIN FUNCTIONS        ##############################

"""
    start(worker::Worker)

Coordinate a worker's startup.
"""
function start(worker::Worker)
    worker.status === :starting || return

    start_listening(worker)
    notice(
        logger,
        "Start worker at: \"$(worker.address)\", " *
        "waiting to connect to: \"$(worker.scheduler_address)\""
    )

    register(worker)
end

"""
    register(worker::Worker)

Register a `Worker` with the dask-scheduler process.
"""
function register(worker::Worker)
    @schedule begin
        worker.status = :connecting
        response = send_recv(
            worker.scheduler,
            Dict{String, Any}(
                "op" => "register",
                "address" => worker.address,
                "ncores" => Sys.CPU_CORES,
                "keys" => collect(keys(worker.data)),
                "now" => time(),
                "in_memory" => length(worker.data),
                "ready" => length(worker.ready),
                "memory_limit" => Sys.total_memory() * 0.6,
            )
        )

        response == "OK" || error("Worker registration failed. Check the scheduler.")
        worker.status = :running
    end
end

"""
    handle_comm(worker::Worker, comm::TCPSocket)

Listen for incoming messages on an established connection.
"""
function handle_comm(worker::Worker, comm::TCPSocket)
    @schedule begin
        while isopen(comm)
            msgs = Dict{String, Any}[]
            try
                msgs = recv_msg(comm)
            catch exception
                # EOFError's are expected when connections are closed unexpectedly
                isa(exception, EOFError) || warn(logger, "Lost connection: $exception.")
                break
            end

            if !isa(msgs, Array)
                if isa(msgs, Dict)
                    msgs = Dict[msgs]
                elseif isa(msgs, Void)
                    continue
                end
            end

            received_new_compute_stream_op = false

            for msg in msgs
                op = pop!(msg, "op", nothing)

                if op != nothing
                    reply = pop!(msg, "reply", nothing)
                    close_desired = pop!(msg, "close", nothing)

                    if op == "close"
                        if reply == "true"
                            send_msg(comm, "OK")
                        end
                        close(comm)
                        break
                    end

                    msg = Dict{Symbol, Any}(parse(k) => v for (k,v) in msg)

                    if haskey(worker.compute_stream_handlers, op)
                        received_new_compute_stream_op = true

                        # Remove unused variables
                        delete!(msg, :resource_restrictions)
                        delete!(msg, :nbytes)
                        delete!(msg, :duration)

                        compute_stream_handler = worker.compute_stream_handlers[op]
                        compute_stream_handler(worker; msg...)

                    elseif op == "compute-stream"
                        if isnull(worker.batched_stream)
                            worker.batched_stream = BatchedSend(comm, interval=0.002)
                        end
                    else
                        handler = worker.handlers[op]
                        result = handler(worker; msg...)

                        if reply == "true"
                            send_msg(comm, result)
                        end

                        if op == "terminate"
                            close(comm)
                            close(worker.listener)
                            return
                        end
                    end

                    if close_desired == "true"
                        close(comm)
                        break
                    end
                end
            end

            if received_new_compute_stream_op == true
                worker.priority_counter -= 1
                ensure_communicating(worker)
                ensure_computing(worker)
            end
        end

        close(comm)
    end
end

"""
    Base.close(worker::Worker; report::String="true")

Close the worker and all the connections it has open.
"""
function Base.close(worker::Worker; report::String="true")
    @schedule begin
        if worker.status ∉ (:closed, :closing)
            worker.status = :closing
            notice(logger, "Stopping worker at $(worker.address)")

            if report == "true"
                response = send_recv(
                    worker.scheduler,
                    Dict{String, String}(
                        "op" => "unregister",
                        "address" => string(worker.address)
                    )
                )
                info(logger, "Scheduler closed connection to worker: \"$response\"")
            end

            isnull(worker.batched_stream) || close(get(worker.batched_stream))
            close(worker.scheduler)

            worker.status = :closed
            close(worker.connection_pool)
        end
    end
end

##############################       HANDLER FUNCTIONS        ##############################

"""
    get_data(worker::Worker; keys::Array=String[], who::String="") -> Dict

Send the results of `keys` back over the stream they were requested on.

# Returns
- `Dict{String, Vector{UInt8}}`: dictionary mapping keys to their serialized data for
    communication
"""
function get_data(worker::Worker; keys::Array=String[], who::String="")
    keys = Vector{String}(keys)
    debug(logger, "\"get_data\": ($keys: \"$who\")")

    return Dict{String, Vector{UInt8}}(
        k =>
        to_serialize(worker.data[k]) for k in filter(k -> haskey(worker.data, k), keys)
    )
end

"""
    gather(worker::Worker; who_has::Dict=Dict{String, Vector{String}}())

Gather the results for various keys.
"""
function gather(worker::Worker; who_has::Dict=Dict{String, Vector{String}}())
    who_has = filter((k,v) -> !haskey(worker.data, k), who_has)

    result, missing_keys, missing_workers = gather_from_workers(
        who_has,
        worker.connection_pool
    )
    if !isempty(missing_keys)
        warn(
            logger,
            "Could not find data: $(keys(missing_keys)) on workers: $missing_workers "
        )

        missing_keys = Dict{Vector{UInt8}, Vector{String}}(
            Vector{UInt8}(k) => v for (k,v) in missing_keys
        )
        return Dict{String, Union{String, Dict{Vector{UInt8},Vector{String}}}}(
            "status" => "missing-data",
            "keys" => missing_keys,
        )
    else
        update_data(worker, data=result, report="false")
        return Dict("status" => "OK")
    end
end


"""
    update_data(worker::Worker; data::Dict=Dict(), report::String="true") -> Dict

Update the worker data.
"""
function update_data(
    worker::Worker;
    data::Dict{String, Any}=Dict{String, Any}(),
    report::String="true"
)::Dict{String, Union{String, Dict{String, Int}}}

    for (key, value) in data
        if haskey(worker.task_state, key)
            transition(worker, key, :memory, value=value)
        else
            put_key_in_memory(worker, key, value)
            worker.task_state[key] = :memory
            worker.dependencies[key] = Set()
        end

        haskey(worker.dep_state, key) && transition_dep(worker, key, :memory, value=value)
        debug(logger, "\"$key: \"receive-from-scatter\"")
    end

    if report == "true"
        send_msg(
            get(worker.batched_stream),
            Dict("op" => "add-keys", "keys" => collect(Vector{UInt8}, keys(data)))
        )
    end

    return Dict{String, Any}(
        "nbytes" => Dict{String, Int}(k => sizeof(v) for (k,v) in data),
        "status" => "OK"
    )
end

"""
    delete_data(worker::Worker; keys::Array=String[], report::String="true")

Delete the data associated with each key of `keys` in `worker.data`.
"""
function delete_data(worker::Worker; keys::Array=String[], report::String="true")
    @schedule begin
        keys = Vector{String}(keys)

        for key in keys
            if haskey(worker.task_state, key)
                release_key(worker, key=key)
            end
            if haskey(worker.dep_state, key)
                release_dep(worker, key)
            end
        end
    end
end

"""
    terminate(worker::Worker; report::String="true")

Shutdown the worker and close all its connections.
"""
function terminate(worker::Worker; report::String="true")
    close(worker, report=report)
    return "OK"
end

"""
    get_keys(worker::Worker) -> Vector{String}

Get a list of all the keys held by this worker for communication with scheduler and other
workers.
"""
get_keys(worker::Worker)::Vector{String} = collect(String, keys(worker.data))

##############################     COMPUTE-STREAM FUNCTIONS    #############################

"""
    add_task(worker::Worker; kwargs...)

Add a task to the worker's list of tasks to be computed.

# Keywords

- `key::String`: The tasks's unique identifier. Throws an exception if blank.
- `priority::Array`: The priority of the task. Throws an exception if blank.
- `who_has::Dict`: Map of dependent keys and the addresses of the workers that have them.
- `func::Union{String, Vector{UInt8}}`: The callable funtion for the task, serialized.
- `args::Union{String, Vector{UInt8}}}`: The arguments for the task, serialized.
- `kwargs::Union{String, Vector{UInt8}}`: The keyword arguments for the task, serialized.
- `future::Union{String, Vector{UInt8}}}`: The tasks's serialized `DeferredFuture`.
"""
function add_task(
    worker::Worker;
    key::String="",
    priority::Array=Vector{String}(),
    who_has::Dict=Dict{String, Vector{String}}(),
    func::Union{String, Vector{UInt8}}=UInt8[],
    args::Union{String, Vector{UInt8}}=UInt8[],
    kwargs::Union{String, Vector{UInt8}}=UInt8[],
    future::Union{String, Vector{UInt8}}=UInt8[],
)

    if get(worker.task_state, key, nothing) === :memory
        info(logger, "Asked to compute pre-existing result: (\"$key\": \"memory\")")
        send_task_state_to_scheduler(worker, key)
        return
    end

    if get(worker.dep_state, key, nothing) === :memory
        worker.task_state[key] = :memory
        debug(logger, "\"$key\": \"new-task-already-in-memory\"")
        send_task_state_to_scheduler(worker, key)
        return
    end

    debug(logger, "\"$key\": \"new-task\"")

    try
        func = Vector{UInt8}(func)
        args = Vector{UInt8}(args)
        kwargs = Vector{UInt8}(kwargs)
        future = Vector{UInt8}(future)

        worker.tasks[key] = deserialize_task(func, args, kwargs, future)
    catch exception
        error_msg = Dict{String, String}(
            "exception" => "$(typeof(exception)))",
            "traceback" => sprint(showerror, exception),
            "key" => key,
            "op" => "task-erred",
        )
        warn(
            logger,
            "Could not deserialize task: (\"$key\": $(error_msg["traceback"]))"
        )
        send_msg(get(worker.batched_stream), error_msg)
        return
    end

    priority = Vector{Int}(map(parse, priority))
    who_has = Dict{String, Vector{String}}(who_has)

    worker.priorities[key] = (priority[1], worker.priority_counter, priority[2])
    worker.task_state[key] = :waiting

    worker.dependencies[key] = Set{String}(keys(who_has))
    worker.waiting_for_data[key] = Set{String}()

    for dep in keys(who_has)
        if !haskey(worker.dependents, dep)
            worker.dependents[dep] = Set{String}()
        end
        push!(worker.dependents[dep], key)

        if !haskey(worker.dep_state, dep)
            if haskey(worker.task_state, dep) && worker.task_state[dep] === :memory
                worker.dep_state[dep] = :memory
            else
                worker.dep_state[dep] = :waiting
            end
        end

        if worker.dep_state[dep] !== :memory
            push!(worker.waiting_for_data[key], dep)
        end
    end

    for (dep, workers) in who_has
        if !haskey(worker.who_has, dep)
            worker.who_has[dep] = Set{String}(workers)
        else
            push!(worker.who_has[dep], workers...)
        end

        for worker_addr in workers
            push!(worker.has_what[worker_addr], dep)
            if worker.dep_state[dep] !== :memory
                push!(worker.pending_data_per_worker[worker_addr], dep)
            end
        end
    end

    if !isempty(worker.waiting_for_data[key])
        push!(worker.data_needed, key)
    else
        transition(worker, key, :ready)
    end
end

"""
    release_key(worker::Worker; key::String="", cause::String="", reason::String="")

Delete a key and its data.
"""
function release_key(worker::Worker; key::String="", cause::String="", reason::String="")

    haskey(worker.task_state, key) || return
    reason == "stolen" && worker.task_state[key] in (:executing, :memory) && return

    state = worker.task_state[key]
    debug(logger, "\"$key\": \"release-key\" $cause")

    delete!(worker.tasks, key)

    if haskey(worker.data, key) && !haskey(worker.dep_state, key)
        delete!(worker.data, key)
    end

    haskey(worker.waiting_for_data, key) && delete!(worker.waiting_for_data, key)

    for dep in pop!(worker.dependencies, key, ())
        if haskey(worker.dependents, dep)
            delete!(worker.dependents[dep], key)
            if isempty(worker.dependents[dep]) && worker.dep_state[dep] === :waiting
                release_dep(worker, dep)
            end
        end
    end

    delete!(worker.priorities, key)

    if state in (:waiting, :ready, :executing) && !isnull(worker.batched_stream)
        send_msg(
            get(worker.batched_stream),
            Dict("op" => "release", "key" => key, "cause" => cause)
        )
    end
end

"""
    release_dep(worker::Worker, dep::String)

Delete a dependency key and its data.
"""
function release_dep(worker::Worker, dep::String)
    haskey(worker.dep_state, dep) || return

    debug(logger, "\"$dep\": \"release-dep\"")
    haskey(worker.dep_state, dep) && pop!(worker.dep_state, dep)

    if !haskey(worker.task_state, dep)
        if haskey(worker.data, dep)
            delete!(worker.data, dep)
        end
    end

    for key in pop!(worker.dependents, dep, ())
        delete!(worker.dependencies[key], dep)
        if !haskey(worker.task_state, key) || worker.task_state[key] !== :memory
            release_key(worker, key=key, cause=dep)
        end
    end
end

##############################       EXECUTING FUNCTIONS      ##############################

"""
    ensure_computing(worker::Worker)

Make sure the worker is computing available tasks.
"""
function ensure_computing(worker::Worker)
    while !isempty(worker.ready)
        key = dequeue!(worker.ready)
        if get(worker.task_state, key, nothing) === :ready
            transition(worker, key, :executing)
        end
    end
end

"""
    execute(worker::Worker, key::String)

Execute the task identified by `key`.
"""
function execute(worker::Worker, key::String)
    @schedule begin
        get(worker.task_state, key, nothing) === :executing || return
        haskey(worker.tasks, key) || return

        (func, args, kwargs, future) = worker.tasks[key]

        args2 = pack_data(args, worker.data)
        kwargs2 = pack_data(kwargs, worker.data)

        get(worker.task_state, key, nothing) === :executing || return

        value = apply_function(key, func, args2, kwargs2)

        # Ensure the task hasn't been released (cancelled) by the scheduler
        haskey(worker.tasks, key) || return

        if !isnull(future)
            try
                df = get(future)
                !isready(df) && put!(df, value)
            catch exception
                notice(logger, "Remote exception on future for key \"$key\": $exception")
            end
        end

        transition(worker, key, :memory, value=value)

        info(logger, "Send compute response to scheduler: \"$key\"")

        ensure_computing(worker)
        ensure_communicating(worker)
    end
end

"""
    put_key_in_memory(worker::Worker, key::String, value; should_transition::Bool=true)

Store the result (`value`) of the task identified by `key`.
"""
function put_key_in_memory(worker::Worker, key::String, value; should_transition::Bool=true)
    haskey(worker.data, key) && return
    worker.data[key] = value

    for dep in get(worker.dependents, key, String[])
        if haskey(worker.waiting_for_data, dep)
            if key in worker.waiting_for_data[dep]
                delete!(worker.waiting_for_data[dep], key)
            end
            if isempty(worker.waiting_for_data[dep])
                transition(worker, dep, :ready)
            end
        end
    end

    if should_transition && haskey(worker.task_state, key)
        transition(worker, key, :memory)
    end

    debug(logger, "\"$key\": \"put-in-memory\"")
end

##############################  PEER DATA GATHERING FUNCTIONS ##############################

"""
    ensure_communicating(worker::Worker)

Ensure the worker is communicating with its peers to gather dependencies as needed.
"""
function ensure_communicating(worker::Worker)
    changed = true
    while changed && !isempty(worker.data_needed)
        changed = false
        info(
            logger,
            "Ensure communicating.  " *
            "Pending: $(length(worker.data_needed)).  " *
            "Connections: $(length(worker.in_flight_workers))"
        )
        key = !isempty(worker.data_needed) ? front(worker.data_needed) : return

        if !haskey(worker.tasks, key) || get(worker.task_state, key, nothing) !== :waiting
            !isempty(worker.data_needed) && key == front(worker.data_needed) && shift!(worker.data_needed)
            changed = true
            continue
        end

        deps = collect(
            filter(dep -> (worker.dep_state[dep] === :waiting), worker.dependencies[key])
        )

        missing_deps = Set{String}(filter(dep -> !haskey(worker.who_has, dep), deps))

        if !isempty(missing_deps)
            warn(logger, "Could not find the dependencies for key \"$key\"")
            missing_deps2 = Set{String}(
                filter(dep -> dep ∉ worker.missing_dep_flight, missing_deps)
            )

            if !isempty(missing_deps2)
                push!(worker.missing_dep_flight, missing_deps2...)
                handle_missing_dep(worker, missing_deps2)
            end

            deps = filter(dependency -> dependency ∉ missing_deps, deps)
        end

        debug(logger, "\"gather-dependencies\": (\"$key\": $deps)")
        in_flight = false

        while !isempty(deps)
            dep = pop!(deps)
            if get(worker.dep_state, dep, "") === :waiting && haskey(worker.who_has, dep)
                workers = collect(
                    filter(w -> !haskey(worker.in_flight_workers, w), worker.who_has[dep])
                )
                if isempty(workers)
                    in_flight = true
                    continue
                end
                worker_addr = rand(workers)
                to_gather = select_keys_for_gather(worker, worker_addr, dep)

                worker.in_flight_workers[worker_addr] = to_gather

                for dep2 in to_gather
                    if get(worker.dep_state, dep2, nothing) === :waiting
                        transition_dep(worker, dep2, :flight, worker_addr=worker_addr)
                    else
                        pop!(to_gather, dep2)
                    end
                end
                gather_dep(worker, worker_addr, dep, to_gather, cause=key)
                changed = true
            end
        end

        if isempty(deps) && !in_flight && !isempty(worker.data_needed)
            key == front(worker.data_needed) && shift!(worker.data_needed)
        end
    end
end

"""
    gather_dep(worker::Worker, worker_addr::String, dep::String, deps::Set; cause::String="")

Gather the dependency with identifier "dep" from `worker_addr`.
"""
function gather_dep(
    worker::Worker,
    worker_addr::String,
    dep::String,
    deps::Set{String};
    cause::String=""
)
    @schedule begin
        worker.status !== :running && return
        response = Dict{String, Any}()

        debug(logger, "\"request-dep\": (\"$dep\", \"$worker_addr\", $deps)")
        info(logger, "Request $(length(deps)) keys")

        try
            response = send_recv(
                worker.connection_pool,
                Address(worker_addr),
                Dict{String, Any}(
                    "op" => "get_data",
                    "reply" => "true",
                    "keys" => collect(String, deps),
                    "who" => worker.address,
                )
            )

            debug(logger, "\"receive-dep\": (\"$worker_addr\", $(keys(response)))")

            if !isempty(response)
                send_msg(
                    get(worker.batched_stream),
                    Dict("op" => "add-keys", "keys" => collect(String, keys(response)))
                )
            end
        catch exception
            warn(
                logger,
                "Worker stream died during communication \"$worker_addr\": $exception"
            )
            debug(logger, "\"received-dep-failed\": \"$worker_addr\"")

            for dep in pop!(worker.has_what, worker_addr)
                delete!(worker.who_has[dep], worker_addr)
                if haskey(worker.who_has, dep) && isempty(worker.who_has[dep])
                    delete!(worker.who_has, dep)
                end
            end
        end

        for dep in pop!(worker.in_flight_workers, worker_addr)
            if haskey(response, dep)
                value = to_deserialize(Vector{UInt8}(response[dep]))
                transition_dep(worker, dep, :memory, value=value)

            elseif !haskey(worker.dep_state, dep) || worker.dep_state[dep] !== :memory
                transition_dep(worker, dep, :waiting, worker_addr=worker_addr)
            end

            if !haskey(response, dep) && haskey(worker.dependents, dep)
                debug(logger, "\"missing-dep\": \"$dep\"")
            end
        end

        ensure_computing(worker)
        ensure_communicating(worker)
    end
end

"""
    handle_missing_dep(worker::Worker, deps::Set{String})

Handle a missing dependency that can't be found on any peers.
"""
function handle_missing_dep(worker::Worker, deps::Set{String})
    @schedule begin
        !isempty(deps) || return
        debug(logger, "\"handle-missing\": $deps")

        missing_deps = filter(dep -> haskey(worker.dependents, dep), deps)

        !isempty(missing_deps) || return
        info(logger, "Dependents not found: $missing_deps. Asking scheduler")

        who_has::Dict{String, Vector{String}} = send_recv(
            worker.scheduler,
            Dict("op" => "who_has", "keys" => collect(String, missing_deps)),
        )
        who_has = filter((k,v) -> !isempty(v), who_has)
        update_who_has(worker, who_has)

        for dep in missing_deps
            if !haskey(who_has, dep)
                dependent = get(worker.dependents, dep, nothing)
                debug(logger, "\"$dep\": (\"no workers found\": \"$dependent\")")
                release_dep(worker, dep)
            else
                debug(logger, "\"$dep\": \"new workers found\"")
                for key in get(worker.dependents, dep, ())
                    if haskey(worker.waiting_for_data, key)
                        push!(worker.data_needed, key)
                    end
                end
            end
        end

        for dep in deps
            delete!(worker.missing_dep_flight, dep)
        end

        ensure_communicating(worker)
    end
end

"""
    update_who_has(worker::Worker, who_has::Dict{String, Vector{String}})

Ensure `who_has` is up to date and accurate.
"""
function update_who_has(worker::Worker, who_has::Dict{String, Vector{String}})
    for (dep, workers) in who_has
        if !isempty(workers)
            if haskey(worker.who_has, dep)
                push!(worker.who_has[dep], workers...)
            else
                worker.who_has[dep] = Set{String}(workers)
            end

            for worker_address in workers
                push!(worker.has_what[worker_address], dep)
            end
        end
    end
end

"""
    select_keys_for_gather(worker::Worker, worker_addr::String, dep::String)

Select which keys to gather from peer at `worker_addr`.
"""
function select_keys_for_gather(worker::Worker, worker_addr::String, dep::String)
    deps = Set{String}([dep])
    pending = worker.pending_data_per_worker[worker_addr]

    while !isempty(pending)
        dep = shift!(pending)

        (!haskey(worker.dep_state, dep) || worker.dep_state[dep] !== :waiting) && continue

        push!(deps, dep)
    end

    return deps
end

"""
    gather_from_workers(who_has::Dict, connection_pool::ConnectionPool) -> Tuple

Gather data directly from `who_has` peers.
"""
function gather_from_workers(
    who_has::Dict,
    connection_pool::ConnectionPool
)::Tuple{Dict{String, Any}, Dict{String, Vector{String}}, Vector{String}}

    bad_addresses = Set{String}()
    missing_workers = Set{String}()
    original_who_has = who_has
    who_has = Dict{String, Set{String}}(k => Set{String}(v) for (k,v) in who_has)
    results = Dict{String, Any}()
    all_bad_keys = Set{String}()

    while length(results) + length(all_bad_keys) < length(who_has)
        directory = Dict{String, Vector{String}}()
        gathered_from = Dict{String, String}()

        for (key, addresses) in who_has

            haskey(results, key) && continue

            if isempty(addresses)
                push!(all_bad_keys, key)
                continue
            end

            possible_addresses = collect(String, setdiff(addresses, bad_addresses))
            if isempty(possible_addresses)
                push!(all_bad_keys, key)
                continue
            end

            address = rand(possible_addresses)
            if !haskey(directory, address)
                directory[address] = String[]
            end
            push!(directory[address], key)
            gathered_from[key] = address
        end

        responses = Dict{String, Any}()
        for (address, keys_to_gather) in directory
            response = Dict{String, Any}()
            try
                response = send_recv(
                    connection_pool,
                    Address(address),
                    Dict(
                        "op" => "get_data",
                        "reply" => true,
                        "keys" => collect(Vector{UInt8}, keys_to_gather),
                        "close" => false,
                    ),
                )
            catch exception
                warn(
                    logger,
                    "Worker stream died during communication \"$address\": $exception"
                )
                push!(missing_workers, address)
            finally
                merge!(responses, response)
            end
        end

        union!(
            bad_addresses,
            Set{String}(v for (k, v) in gathered_from if !haskey(responses, k))
        )
        merge!(results, responses)
    end

    bad_keys = Dict{String, Vector{String}}(k => original_who_has[k] for k in all_bad_keys)

    return results, bad_keys, collect(String, missing_workers)
end

##############################      TRANSITION FUNCTIONS      ##############################

"""
    transition(worker::Worker, key::String, finish_state::Symbol; kwargs...)

Transition task with identifier `key` to finish_state from its current state.
"""
function transition(worker::Worker, key::String, finish_state::Symbol; kwargs...)
    # Ensure the task hasn't been released (cancelled) by the scheduler
    if haskey(worker.task_state, key)
        start_state = worker.task_state[key]

        if start_state != finish_state
            transition_func = worker.transitions[start_state, finish_state]
            transition_func(worker, key; kwargs...)
            worker.task_state[key] = finish_state
        end
    end
end

function transition_waiting_ready(worker::Worker, key::String)
    delete!(worker.waiting_for_data, key)
    haskey(worker.ready, key) || enqueue!(worker.ready, key, worker.priorities[key])
    delete!(worker.priorities, key)
end

function transition_waiting_done(worker::Worker, key::String; value::Any=nothing)
    delete!(worker.waiting_for_data, key)
    send_task_state_to_scheduler(worker, key)
    delete!(worker.tasks, key)
end

function transition_ready_executing(worker::Worker, key::String)
    execute(worker, key)
end

function transition_ready_done(worker::Worker, key::String; value::Any=nothing)
    send_task_state_to_scheduler(worker, key)
    delete!(worker.tasks, key)
end

function transition_executing_done(worker::Worker, key::String; value::Any=no_value)
    if value != no_value
        put_key_in_memory(worker, key, value, should_transition=false)
        haskey(worker.dep_state, key) && transition_dep(worker, key, :memory)
    end
    delete!(worker.tasks, key)

    send_task_state_to_scheduler(worker, key)
end

"""
    transition_dep(worker::Worker, dep::String, finish_state::Symbol; kwargs...)

Transition dependency task with identifier `key` to finish_state from its current state.
"""
function transition_dep(worker::Worker, dep::String, finish_state::Symbol; kwargs...)
    if haskey(worker.dep_state, dep)
        start_state = worker.dep_state[dep]

        if start_state != finish_state && !(start_state === :memory && finish_state === :flight)
            func = worker.dep_transitions[(start_state, finish_state)]
            func(worker, dep; kwargs...)
            debug(logger, "\"$dep\": transition dependency $start_state => $finish_state")
        end
    end
end

function transition_dep_waiting_flight(worker::Worker, dep::String; worker_addr::String="")
    worker.dep_state[dep] = :flight
end

function transition_dep_flight_waiting(worker::Worker, dep::String; worker_addr::String="")

    haskey(worker.who_has, dep) && delete!(worker.who_has[dep], worker_addr)
    haskey(worker.has_what, worker_addr) && delete!(worker.has_what[worker_addr], dep)

    if !haskey(worker.who_has, dep) || isempty(worker.who_has[dep])
        if dep ∉ worker.missing_dep_flight
            push!(worker.missing_dep_flight, dep)
            handle_missing_dep(worker, Set([dep]))
        end
    end

    for key in get(worker.dependents, dep, ())
        if worker.task_state[key] === :waiting
            unshift!(worker.data_needed, key)
        end
    end

    if haskey(worker.dependents, dep) && isempty(worker.dependents[dep])
        release_dep(worker, dep)
    end
    worker.dep_state[dep] = :waiting
end

function transition_dep_flight_memory(worker::Worker, dep::String; value=nothing)
    worker.dep_state[dep] = :memory
    put_key_in_memory(worker, dep, value)
end


function transition_dep_waiting_memory(worker::Worker, dep::String; value=nothing)
    worker.dep_state[dep] = :memory
end

##############################      SCHEDULER FUNCTIONS       ##############################

"""
    send_task_state_to_scheduler(worker::Worker, key::String)

Send the state of task `key` to the scheduler.
"""
function send_task_state_to_scheduler(worker::Worker, key::String)
    haskey(worker.data, key) || return

    send_msg(
        get(worker.batched_stream),
        Dict{String, Union{String, Int}}(
            "op" => "task-finished",
            "status" => "OK",
            "key" => key,
            "nbytes" => sizeof(worker.data[key]),
        )
    )
end

##############################         OTHER FUNCTIONS        ##############################

"""
    deserialize_task(func, args, kwargs, future) -> Tuple

Deserialize task inputs and regularize to func, args, kwargs.

# Returns
- `Tuple`: The deserialized function, arguments, keyword arguments, and Deferredfuture for
the task.
"""
function deserialize_task(
    func::Vector{UInt8},
    args::Vector{UInt8},
    kwargs::Vector{UInt8},
    future::Vector{UInt8}
)::Tuple{Base.Callable, Tuple, Vector{Any}, Nullable{DeferredFuture}}

    func = to_deserialize(func)
    args = to_deserialize(args)
    kwargs = isempty(kwargs) ? Vector{Any}() : to_deserialize(kwargs)
    future = isempty(future) ? Nullable() : to_deserialize(future)

    return (func, args, kwargs, future)
end

"""
    apply_function(key::String, func::Base.Callable, args::Any, kwargs::Any)

Run a function and return collected information.
"""
function apply_function(key::String, func::Base.Callable, args::Tuple, kwargs::Vector{Any})
    try
        result = func(args..., kwargs...)
        return result
    catch exception
        # Necessary because of a bug with empty stacktraces
        # in base, but will be fixed in 0.6
        # see https://github.com/JuliaLang/julia/issues/19655
        trace = try
            catch_stacktrace()
        catch
            StackFrame[]
        end
        result = (exception => trace)
        warn(logger, "Compute Failed for \"$key\": $result")
        return result
    end
end
