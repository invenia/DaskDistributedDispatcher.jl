const no_value = "--no-value-sentinel--"


"""
    Worker

A `Worker` represents a worker endpoint in the distributed cluster that accepts instructions
from the scheduler, fetches dependencies, executes compuations, stores data, and
communicates state to the scheduler.

# Fields

## Server
- `address::Address`:: ip address and port that this worker is listening on
- `listener::Base.TCPServer`: tcp server that listens for incoming connections

## Communication Management
- `scheduler_address::Address`: the dask-distributed scheduler ip address and port information
- `batched_stream::Nullable{BatchedSend}`: batched stream for communication with scheduler
- `scheduler::Rpc`: manager for discrete send/receive open connections to the scheduler
- `target_message_size::AbstractFloat`: target message size for messages
- `connection_pool::ConnectionPool`: manages connections to peers
- `total_connections::Integer`: maximum number of concurrent connections allowed

## Handlers
- `handlers::Dict{String, Function}`: handlers for operations requested by open connections
- `compute_stream_handlers::Dict{String, Function}`: handlers for compute stream operations

## Data management
- `data::Dict{String, Any}`: maps keys to the results of function calls (actual values)
- `futures::Dict{String, DeferredFutures.DeferredFuture}`: maps keys to their DeferredFuture
- `nbytes::Dict{String, Integer}`: maps keys to the size of their data
- `types::Dict{String, Type}`: maps keys to the type of their data

## Task management
- `tasks::Dict{String, Tuple}`: maps keys to the function, args, and kwargs of a task
- `task_state::Dict{String, String}`: maps keys tot heir state: (waiting, executing, memory)
- `priorities::Dict{String, Tuple}`: run time order priority of a key given by the scheduler
- `priority_counter::Integer`: used to also prioritize tasks by their order of arrival

## Task state management
- `transitions::Dict{Tuple, Function}`: valid transitions that a task can make
- `data_needed::Deque{String}`: keys whose data we still lack
- `ready::PriorityQueue{String, Tuple, Base.Order.ForwardOrdering}`: keys ready to run
- `executing::Set{String}`: keys that are currently executing

## Dependency management
- `dep_transitions::Dict{Tuple, Function}`: valid transitions that a dependency can make
- `dep_state::Dict{String, String}`: maps dependencies with their state
    (waiting, flight, memory)
- `dependencies::Dict{String, Set}`: maps a key to the data it needs to run
- `dependents::Dict{String, Set}`: maps a dependency to the keys that use it
- `waiting_for_data::Dict{String, Set}`: maps a key to the data it needs that we don't have
- `pending_data_per_worker::DefaultDict{String, Deque}`: data per worker that we want
- `who_has::Dict{String, Set}`: maps keys to the workers believed to have their data
- `has_what::DefaultDict{String, Set{String}}`: maps workers to the data they have

# Peer communication
- `in_flight_tasks::Dict{String, String}`: maps a dependency and the peer connection for it
- `in_flight_workers::Dict{String, Set}`: workers from which we are getting data from
- `suspicious_deps::DefaultDict{String, Integer}`: number of times a dependency has not been
    where it is expected
- `missing_dep_flight::Set{String}`: missing dependencies

### Informational
- `status::String`: status of this worker
- `exceptions::Dict{String, String}`: maps erred keys to the exception thrown while running
- `tracebacks::Dict{String, String}`: maps erred keys to the exception's traceback thrown
- `startstops::DefaultDict{String, Array}`: logs of transfer, load, and compute times

### Validation
- `validate::Bool`: decides if the worker validates its state during execution
"""
type Worker <: Server
    # Server
    address::Address
    listener::Base.TCPServer

    # Communication management
    scheduler_address::Address
    batched_stream::Nullable{BatchedSend}
    scheduler::Rpc
    target_message_size::AbstractFloat
    connection_pool::ConnectionPool
    total_connections::Integer

    # Handlers
    handlers::Dict{String, Function}
    compute_stream_handlers::Dict{String, Function}

    # Data  management
    data::Dict{String, Any}
    futures::Dict{String, DeferredFutures.DeferredFuture}
    nbytes::Dict{String, Integer}
    types::Dict{String, Type}

    # Task management
    tasks::Dict{String, Tuple}
    task_state::Dict{String, String}
    priorities::Dict{String, Tuple}
    priority_counter::Integer

    # Task state management
    transitions::Dict{Tuple{String, String}, Function}
    data_needed::Deque{String}
    ready::PriorityQueue{String, Tuple, Base.Order.ForwardOrdering}
    executing::Set{String}

    # Dependency management
    dep_transitions::Dict{Tuple{String, String}, Function}
    dep_state::Dict{String, String}
    dependencies::Dict{String, Set}
    dependents::Dict{String, Set}
    waiting_for_data::Dict{String, Set}
    pending_data_per_worker::DefaultDict{String, Deque{String}}
    who_has::Dict{String, Set{String}}
    has_what::DefaultDict{String, Set{String}}

    # Peer communication
    in_flight_tasks::Dict{String, String}
    in_flight_workers::Dict{String, Set{String}}
    suspicious_deps::DefaultDict{String, Integer}
    missing_dep_flight::Set{String}

    # Informational
    status::String
    exceptions::Dict{String, String}
    tracebacks::Dict{String, String}
    startstops::DefaultDict{String, Array}

    # Validation
    validate::Bool
end

"""
    Worker(scheduler_address::String; validate=true)

Create a `Worker` that listens on a random port between 1024 and 9000 for incoming
messages. Set `validate` to false to improve performance.
"""
function Worker(scheduler_address::String; validate=true)
    scheduler_address = Address(scheduler_address)

    chosen_port = rand(1024:9000)
    actual_port, listener = listenany(chosen_port)
    worker_address = Address(getipaddr(), actual_port)

    # This is the minimal set of handlers needed
    # https://github.com/JuliaParallel/Dagger.jl/issues/53
    handlers = Dict{String, Function}(
        "compute-stream" => compute_stream,
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
    transitions = Dict{Tuple{String, String}, Function}(
        ("waiting", "ready") => transition_waiting_ready,
        ("waiting", "memory") => transition_waiting_done,
        ("waiting", "error") => transition_waiting_done,
        ("ready", "executing") => transition_ready_executing,
        ("ready", "memory") => transition_ready_memory,
        ("executing", "memory") => transition_executing_done,
        ("executing", "error") => transition_executing_done,
    )
    dep_transitions = Dict{Tuple{String, String}, Function}(
        ("waiting", "flight") => transition_dep_waiting_flight,
        ("waiting", "memory") => transition_dep_waiting_memory,
        ("flight", "waiting") => transition_dep_flight_waiting,
        ("flight", "memory") => transition_dep_flight_memory,
    )
    worker = Worker(
        worker_address,
        listener,

        scheduler_address,
        nothing, #  batched_stream
        Rpc(scheduler_address),  # scheduler
        50e6,  # target_message_size = 50 MB
        ConnectionPool(),  # connection_pool
        50,  # total_connections

        handlers,
        compute_stream_handlers,

        Dict{String, Any}(),  # data
        Dict{String, DeferredFutures.DeferredFuture}(), # futures
        Dict{String, Integer}(),  # nbytes
        Dict{String, Type}(),  # types

        Dict{String, Tuple}(),  # tasks
        Dict{String, String}(),  #task_state
        Dict{String, Tuple}(),  # priorities
        0,  # priority_counter

        transitions,
        Deque{String}(),  # data_needed
        PriorityQueue(String, Tuple, Base.Order.ForwardOrdering()),  # ready
        Set{String}(),  # executing

        dep_transitions,
        Dict{String, String}(),  # dep_state
        Dict{String, Set}(),  # dependencies
        Dict{String, Set}(),  # dependents
        Dict{String, Set}(),  # waiting_for_data
        DefaultDict{String, Deque{String}}(Deque{String}),  # pending_data_per_worker
        Dict{String, Set{String}}(),  # who_has
        DefaultDict{String, Set{String}}(Set{String}),  # has_what

        Dict{String, String}(),  # in_flight_tasks
        Dict{String, Set{String}}(),  # in_flight_workers
        DefaultDict{String, Integer}(0),  # suspicious_deps
        Set{String}(),  # missing_dep_flight

        "starting",  # status
        Dict{String, String}(),  # exceptions
        Dict{String, String}(),  # tracebacks
        DefaultDict{String, Array}(Array{Any, 1}),  # startstops

        validate,  # validation
    )

    start_worker(worker)
    return worker
end

"""
    shutdown(workers::Array{Address, 1})

Connect to and terminate all workers in `workers`.
"""
function shutdown(workers::Array{Address, 1})
    closed = Array{Address, 1}()
    for worker_address in workers
        clientside = connect(worker_address)
        msg = Dict("op" => "terminate", "reply" => true)
        response = send_recv(clientside, msg)
        if response != "OK"
            error("Unable to shutdown worker at: \"$worker_address\"")
        else
            push!(closed, worker_address)
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
        "<%s: %s, %s, stored: %d, running: %d, ready: %d, comm: %d, waiting: %d>",
        typeof(worker).name.name, worker.address, worker.status,
        length(worker.data), length(worker.executing),
        length(worker.ready), length(worker.in_flight_tasks),
        length(worker.waiting_for_data),
    )
end

##############################         ADMIN FUNCTIONS        ##############################

"""
    start_worker(worker::Worker)

Coordinate a worker's startup.
"""
function start_worker(worker::Worker)
    @assert worker.status == "starting"

    start_listening(worker)

    notice(
        logger,
        "Start worker at: \"$(worker.address)\", " *
        "waiting to connect to: \"$(worker.scheduler_address)\""
    )

    register_worker(worker)
end

"""
    register_worker(worker::Worker)

Register a `Worker` with the dask-scheduler process.
"""
function register_worker(worker::Worker)
    @async begin
        response = send_recv(
            worker.scheduler,
            Dict(
                "op" => "register",
                "address" => worker.address,
                "ncores" => Sys.CPU_CORES,
                "keys" => collect(keys(worker.data)),
                "nbytes" => worker.nbytes,
                "now" => time(),
                "executing" => length(worker.executing),
                "in_memory" => length(worker.data),
                "ready" => length(worker.ready),
                "in_flight" => length(worker.in_flight_tasks),
                "memory_limit" => Sys.total_memory() * 0.6,
                "services" => Dict(),
            )
        )
        try
            @assert response == "OK"
            worker.status = "running"
        catch
            error("An error ocurred on the dask-scheduler while registering this worker.")
        end
    end
end


"""
    handle_comm(worker::Worker, comm::TCPSocket)

Listen for incoming messages on an established connection.
"""
function handle_comm(worker::Worker, comm::TCPSocket)
    @async begin
        incoming_host, incoming_port = getsockname(comm)
        incoming_address = Address(incoming_host, incoming_port)
        info(logger, "Connection received from \"$incoming_address\"")

        op = ""
        is_computing = false

        while isopen(comm)
            msgs = []
            try
                msgs = recv_msg(comm)
             catch exception
                warn(
                    logger,
                    "Lost connection to \"$incoming_address\" " *
                    "while reading message: $exception. " *
                    "Last operation: \"$op\""
                )
                break
            end

            if isa(msgs, Dict)
                msgs = [msgs]
            end

            received_new_compute_stream_op = false

            for msg in msgs
                op = pop!(msg, "op", nothing)

                if op != nothing
                    reply = pop!(msg, "reply", nothing)
                    close_desired = pop!(msg, "close", nothing)

                    if op == "close"
                        if reply == "true" || reply == true
                            send_msg(comm, "OK")
                        end
                        close(comm)
                        break
                    end

                    msg = Dict(parse(k) => v for (k,v) in msg)

                    if is_computing && haskey(worker.compute_stream_handlers, op)
                        received_new_compute_stream_op = true

                        compute_stream_handler = worker.compute_stream_handlers[op]
                        compute_stream_handler(worker, comm; msg...)
                    else
                        if op == "compute-stream"
                            is_computing = true
                        end

                        handler = worker.handlers[op]
                        result = handler(worker, comm; msg...)

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
    Base.close(worker::Worker; reply_comm=nothing, report::Bool=true)

Close the worker and all the connections it has open.
"""
function Base.close(worker::Worker; reply_comm=nothing, report::Bool=true)
    @async begin
        if worker.status ∉ ("closed", "closing")
            notice(logger, "Stopping worker at $(worker.address)")
            worker.status = "closing"

            if report
                response = send_recv(
                    worker.scheduler,
                    Dict("op" => "unregister", "address" => worker.address)
                )
                info(logger, "Scheduler closed connection to worker: \"$response\"")
            end

            isnull(worker.batched_stream) || close(get(worker.batched_stream))
            close(worker.scheduler)

            worker.status = "closed"
            close(worker.connection_pool)
        end
    end
end

##############################       HANDLER FUNCTIONS        ##############################

"""
    compute_stream(worker::Worker, comm::TCPSocket)

Start a batched communication stream to the scheduler.
"""
function compute_stream(worker::Worker, comm::TCPSocket)
    @async begin
        if isnull(worker.batched_stream)
            worker.batched_stream = BatchedSend(comm, interval=0.002)
        end
    end
end

"""
    get_data(worker::Worker, comm::TCPSocket; keys::Array=[], who::String="")

Send the results of `keys` back over the stream they were requested on.
"""
function get_data(worker::Worker, comm::TCPSocket; keys::Array=[], who::String="")
    data = Dict(
        to_key(k) =>
        to_serialize(worker.data[k]) for k in filter(k -> haskey(worker.data, k), keys)
    )
    debug(logger, "\"get_data\": ($keys: \"$who\")")
    return data
end

"""
    gather(worker::Worker, comm::TCPSocket; who_has::Dict=Dict())

Gather the results for various keys.
"""
function gather(worker::Worker, comm::TCPSocket; who_has::Dict=Dict())
    who_has = filter((k,v) -> !haskey(worker.data, k), worker.who_has)

    result, missing_keys, missing_workers = gather_from_workers(
        who_has,
        worker.connection_pool
    )
    if !isempty(missing_keys)
        warn(
            logger,
            "Could not find data: $missing_keys on workers: $missing_workers " *
            "(who_has: $who_has)"
        )
        return Dict("status" => "missing-data", "keys" => missing_keys)
    else
        update_data(worker, comm, data=result, report=false)
        return Dict("status" => "OK")
    end
end


"""
    update_data(worker::Worker, comm::TCPSocket; data::Dict=Dict(), report=true)

Update the worker data.
"""
function update_data(worker::Worker, comm::TCPSocket; data::Dict=Dict(), report=true)
    for (key, value) in data
        if haskey(worker.task_state, key)
            transition(worker, key, "memory", value=value)
        else
            put_key_in_memory(worker, key, value)
            worker.task_state[key] = "memory"
            worker.tasks[key] = nothing
            worker.priorities[key] = nothing
            worker.durations[key] = nothing
            worker.dependencies[key] = Set()
        end

        if haskey(worker.dep_state, key)
            transition_dep(worker, key, "memory", value=value)
        end

        debug(logger, "\"$key: \"receive-from-scatter\"")
    end

    if report
        send_msg(
            get(worker.batched_stream),
            Dict("op" => "add-keys", "keys" => collect(keys(data)))
        )
    end

    return Dict("nbytes" => Dict(k => sizeof(v) for (k,v) in data), "status" => "OK")
end

"""
    delete_data(worker::Worker, comm::TCPSocket; keys::Array=[], report::String="true")

Delete the data associated with each key of `keys` in `worker.data`.
"""
function delete_data(worker::Worker, comm::TCPSocket; keys::Array=[], report::String="true")
    @async begin
        for key in keys
            if haskey(worker.task_state, key)
                release_key(worker, nothing, key=key)
            end
            if haskey(worker.dep_state, key)
                release_dep(worker, key)
            end
        end
    end
end

"""
    terminate(worker::Worker, comm::TCPSocket; report::String="true")

Shutdown the worker and close all its connections.
"""
function terminate(worker::Worker, comm::TCPSocket; report::String="true")
    close(worker, reply_comm=comm, report=parse(report))
    return "OK"
end

"""
    get_keys(worker::Worker, comm::TCPSocket, msg::Dict) -> Array

Get a list of all the keys held by this worker.
"""
function get_keys(worker::Worker, comm::TCPSocket, msg::Dict)
    return collect(keys(worker.data))
end

##############################     COMPUTE-STREAM FUNCTIONS    #############################

"""
    add_task(worker::Worker, comm::TCPSocket; kwargs...)

Add a task to the worker's list of tasks to be computed.

# Keywords

- `key::String`: The tasks's unique identifier. Throws an exception if blank.
- `priority::Array`: The priority of the task. Throws an exception if blank.
- `who_has::Dict`: Map of dependent keys and the addresses of the workers that have them.
- `nbytes::Dict`: Map of the number of bytes of the dependent key's data.
- `duration::String`: The estimated computation cost of the given key. Defaults to "0.5".
- `resource_restrictions::Dict`: Resources required by a task. Should always be an empty Dict.
- `func::Union{String, Array{UInt8,1}}`: The callable funtion for the task, serialized.
- `args::Union{String, Array{UInt8,1}}`: The arguments for the task, serialized.
- `kwargs::Union{String, Array{UInt8,1}}`: The keyword arguments for the task, serialized.
- `future::Union{String, Array{UInt8,1}}`: The tasks's serialized `DeferredFuture`.
"""
function add_task(
    worker::Worker,
    comm::TCPSocket;
    key::String="",
    priority::Array=[],
    who_has::Dict=Dict(),
    nbytes::Dict=Dict(),
    duration::String="0.5",
    resource_restrictions::Dict=Dict(),
    func::Union{String, Array{UInt8,1}}="",
    args::Union{String, Array{UInt8,1}}="",
    kwargs::Union{String, Array{UInt8,1}}="",
    future::Union{String, Array{UInt8,1}}="",
)
    if worker.validate
        @assert key != ""
        @assert !isempty(priority)
        @assert isempty(resource_restrictions)
    end

    priority = map(parse, priority)
    insert!(priority, 2, worker.priority_counter)
    priority = tuple(priority...)

    if haskey(worker.tasks, key)
        state = worker.task_state[key]
        if state in ("memory", "error")
            if state == "memory"
                @assert haskey(worker.data, key)
            end
            info(logger, "Asked to compute pre-existing result: (\"$key\": \"$state\")")
            send_task_state_to_scheduler(worker, key)
        end
        return
    end

    if haskey(worker.dep_state, key) && worker.dep_state[key] == "memory"
        worker.task_state[key] = "memory"
        send_task_state_to_scheduler(worker, key)
        worker.tasks[key] = ()
        debug(logger, "\"$key\": \"new-task-already-in-memory\"")
        worker.priorities[key] = priority
        return
    end

    debug(logger, "\"$key\": \"new-task\"")
    try
        start_time = time()
        worker.tasks[key] = deserialize_task(func, args, kwargs)
        stop_time = time()

        if stop_time - start_time > 0.010
            push!(worker.startstops[key], ("deserialize", start_time, stop_time))
        end
    catch exception
        error_msg = Dict(
            "exception" => "$(typeof(exception)))",
            "traceback" => sprint(showerror, exception),
            "key" => to_key(key),
            "op" => "task-erred",
        )
        warn(
            logger,
            "Could not deserialize task with key: (\"$key\": $(error_msg["traceback"]))"
        )
        send_msg(get(worker.batched_stream), error_msg)
        debug(logger, "\"$key\": \"deserialize-error\"")
        return
    end

    if !isempty(future)
        worker.futures[key] = to_deserialize(future)
    end

    worker.priorities[key] = priority
    worker.task_state[key] = "waiting"

    if !isempty(nbytes)
        for (k,v) in nbytes
            worker.nbytes[k] = parse(v)
        end
    end

    worker.dependencies[key] = Set(keys(who_has))
    worker.waiting_for_data[key] = Set()

    for dep in keys(who_has)
        if !haskey(worker.dependents, dep)
            worker.dependents[dep] = Set()
        end
        push!(worker.dependents[dep], key)

        if !haskey(worker.dep_state, dep)
            if haskey(worker.task_state, dep) && worker.task_state[dep] == "memory"
                worker.dep_state[dep] = "memory"
            else
                worker.dep_state[dep] = "waiting"
            end
        end

        if worker.dep_state[dep] != "memory"
            push!(worker.waiting_for_data[key], dep)
        end
    end

    for (dep, workers) in who_has
        @assert !isempty(workers)
        if !haskey(worker.who_has, dep)
            worker.who_has[dep] = Set(workers)
        end
        push!(worker.who_has[dep], workers...)

        for worker_addr in workers
            push!(worker.has_what[worker_addr], dep)
            if worker.dep_state[dep] != "memory"
                push!(worker.pending_data_per_worker[worker_addr], dep)
            end
        end
    end

    if !isempty(worker.waiting_for_data[key])
        push!(worker.data_needed, key)
    else
        transition(worker, key, "ready")
    end

    if worker.validate && !isempty(who_has)
        @assert all(dep -> haskey(worker.dep_state, dep), keys(who_has))
        @assert all(dep -> haskey(worker.nbytes, dep), keys(who_has))
    end
end

"""
    release_key(worker::Worker; comm::TCPSocket, key::String, cause::String, reason::String)

Delete a key and its data.
"""
function release_key(
    worker::Worker,
    comm::Any;
    key::String="",
    cause::String="",
    reason::String=""
)
    if worker.validate
        @assert key != ""
    end

    if haskey(worker.task_state, key)
        if !(reason == "stolen" && worker.task_state[key] in ("executing", "memory"))
            state = pop!(worker.task_state, key)

            debug(logger, "\"$key\": \"release-key\" $cause")

            delete!(worker.tasks, key)
            if haskey(worker.data, key) && !haskey(worker.dep_state, key)
                delete!(worker.data, key)
                delete!(worker.nbytes, key)
                delete!(worker.types, key)
            end

            haskey(worker.waiting_for_data, key) && delete!(worker.waiting_for_data, key)

            for dep in pop!(worker.dependencies, key, ())
                if haskey(worker.dependents, dep)
                    delete!(worker.dependents[dep], key)
                    if isempty(worker.dependents[dep]) && worker.dep_state[dep] == "waiting"
                        release_dep(worker, dep)
                    end
                end
            end

            delete!(worker.priorities, key)

            haskey(worker.exceptions, key) && delete!(worker.exceptions, key)
            haskey(worker.tracebacks, key) && delete!(worker.tracebacks, key)
            haskey(worker.startstops, key) && delete!(worker.startstops, key)

            if key in worker.executing
                delete!(worker.executing, key)
            end

            if state in ("waiting", "ready", "executing")  # Task is not finished yet
                send_msg(
                    get(worker.batched_stream),
                    Dict("op" => "release", "key" => to_key(key), "cause" => cause)
                )
            end
        end
    end
end

"""
    release_dep(worker::Worker, dep::String)

Delete a dependency key and its data.
"""
function release_dep(worker::Worker, dep::String)
    if haskey(worker.dep_state, dep)
        debug(logger, "\"$dep\": \"release-dep\"")
        haskey(worker.dep_state, dep) && pop!(worker.dep_state, dep)

        haskey(worker.suspicious_deps, dep) && delete!(worker.suspicious_deps, dep)

        if !haskey(worker.task_state, dep)
            if haskey(worker.data, dep)
                delete!(worker.data, dep)
                delete!(worker.types, dep)
            end
            delete!(worker.nbytes, dep)
        end

        haskey(worker.in_flight_tasks, dep) && delete!(worker.in_flight_tasks, dep)

        for key in pop!(worker.dependents, dep, ())
            delete!(worker.dependencies[key], dep)
            if !haskey(worker.task_state, key) || worker.task_state[key] != "memory"
                release_key(worker, nothing, key=key, cause=dep)
            end
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
        if get(worker.task_state, key, nothing) == "ready"
            transition(worker, key, "executing")
        end
    end
end

"""
    execute(worker::Worker, key::String, report=false)

Execute the task identified by `key`. Reports results to scheduler if report=true.
"""
function execute(worker::Worker, key::String, report=false)
    @async begin
        if key in worker.executing && haskey(worker.task_state, key)
            if worker.validate
                @assert !haskey(worker.waiting_for_data, key)
                @assert worker.task_state[key] == "executing"
            end

            (func, args, kwargs) = worker.tasks[key]

            start_time = time()
            args2 = pack_data(args, worker.data, key_types=String)
            kwargs2 = pack_data(kwargs, worker.data, key_types=String)
            stop_time = time()

            if stop_time - start_time > 0.005
                push!(worker.startstops[key], ("disk-read", start_time, stop_time))
            end

            result = apply_function(func, args2, kwargs2)

            if get(worker.task_state, key, nothing) != "executing"
                return
            end

            result["key"] = key
            value = pop!(result, "result", nothing)

            push!(worker.startstops[key], ("compute", result["start"], result["stop"]))

            # Ensure the task hasn't been released (cancelled) by the scheduler
            if haskey(worker.tasks, key)
                if result["op"] == "task-finished"
                    try
                        !isready(worker.futures[key]) && put!(worker.futures[key], value)
                    catch exception
                        notice(
                            logger,
                            "Remote exception on future for key \"$key\": $exception"
                        )
                    end
                    worker.nbytes[key] = result["nbytes"]
                    worker.types[key] = result["type"]
                    transition(worker, key, "memory", value=value)
                else
                    try
                        if !isready(worker.futures[key])
                            put!(worker.futures[key], ("error" => result["exception"]))
                        end
                    catch exception
                        notice(
                            logger,
                            "Remote exception on future for key \"$key\": $exception"
                        )
                    end
                    worker.exceptions[key] = result["exception"]
                    worker.tracebacks[key] = result["traceback"]
                    warn(
                        logger,
                        "Compute Failed for key \"$key\": ($func, $args2, $kwargs2). " *
                        "Traceback: $(result["traceback"])"
                    )
                    transition(worker, key, "error")
                end

                info(
                    logger,
                    "Send compute response to scheduler: (\"$key\": \"$(result["op"])\")"
                )
            end

            if worker.validate
                @assert key ∉ worker.executing
                @assert !haskey(worker.waiting_for_data, key)
            end

            ensure_computing(worker)
            ensure_communicating(worker)
        end
    end
end

"""
    put_key_in_memory(worker::Worker, key::String, value; should_transition::Bool=true)

Store the result (`value`) of the task identified by `key`.
"""
function put_key_in_memory(worker::Worker, key::String, value; should_transition::Bool=true)
    if !haskey(worker.data, key)
        worker.data[key] = value

        if !haskey(worker.nbytes, key)
            worker.nbytes[key] = sizeof(value)
        end

        worker.types[key] = typeof(value)

        for dep in get(worker.dependents, key, [])
            if haskey(worker.waiting_for_data, dep)
                if key in worker.waiting_for_data[dep]
                    delete!(worker.waiting_for_data[dep], key)
                end
                if isempty(worker.waiting_for_data[dep])
                    transition(worker, dep, "ready")
                end
            end
        end

        if should_transition && haskey(worker.task_state, key)
            transition(worker, key, "memory")
        end

        debug(logger, "\"$key\": \"put-in-memory\"")
    end
end

##############################  PEER DATA GATHERING FUNCTIONS ##############################

"""
    ensure_communicating(worker::Worker)

Ensure the worker is communicating with its peers to gather dependencies as needed.
"""
function ensure_communicating(worker::Worker)
    changed = true
    while (
        changed &&
        !isempty(worker.data_needed) &&
        length(worker.in_flight_workers) < worker.total_connections
    )
        changed = false
        info(
            logger,
            "Ensure communicating.  " *
            "Pending: $(length(worker.data_needed)).  " *
            "Connections: $(length(worker.in_flight_workers))/$(worker.total_connections)"
        )

        key = !isempty(worker.data_needed) ? front(worker.data_needed) : nothing

        if key == nothing
            return
        end

        if !haskey(worker.tasks, key)
            !isempty(worker.data_needed) && key == front(worker.data_needed) && shift!(worker.data_needed)
            changed = true
            continue
        end

        if !haskey(worker.task_state, key) || worker.task_state[key] != "waiting"
            debug(logger, "\"$key\": \"communication pass\"")
            !isempty(worker.data_needed) && key == front(worker.data_needed) && shift!(worker.data_needed)
            changed = true
            continue
        end

        deps = worker.dependencies[key]
        if worker.validate
            @assert all(dep -> haskey(worker.dep_state, dep), deps)
        end

        deps = collect(filter(dep -> (worker.dep_state[dep] == "waiting"), deps))

        missing_deps = Set(filter(dep -> !haskey(worker.who_has, dep), deps))

        if !isempty(missing_deps)
            warn(logger, "Could not find the dependencies for key \"$key\"")
            missing_deps2 = Set(filter(dep -> dep ∉ worker.missing_dep_flight, missing_deps))

            for dep in missing_deps2
                push!(worker.missing_dep_flight, dep)
            end

            if !isempty(missing_deps2)
                handle_missing_dep(worker, missing_deps2)
            end

            deps = collect(filter(dep -> dep ∉ missing_deps, deps))
        end

        debug(logger, "\"gather-dependencies\": (\"$key\": $deps)")
        in_flight = false

        while (
            !isempty(deps) && length(worker.in_flight_workers) < worker.total_connections
        )
            dep = pop!(deps)
            if worker.dep_state[dep] == "waiting" && haskey(worker.who_has, dep)
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
                    if get(worker.dep_state, dep2, nothing) == "waiting"
                        transition_dep(worker, dep2, "flight", worker_addr=worker_addr)
                    else
                        pop!(to_gather, dep2)
                    end
                end
                @sync gather_dep(worker, worker_addr, dep, to_gather, cause=key)
                changed = true
            end
        end

        if isempty(deps) && !in_flight && !isempty(worker.data_needed)
            key == front(worker.data_needed) && shift!(worker.data_needed)
        end
    end
end

"""
    gather_dep(worker::Worker, worker_addr::String, dep::String, deps::Set; cause="")

Gather the dependency with identifier "dep" from `worker_addr`.
"""
function gather_dep(worker::Worker, worker_addr::String, dep::String, deps::Set; cause="")
    @async begin
        if worker.status != "running"
            return
        end

        response = Dict()

        if worker.validate
            validate_state(worker)
        end

        debug(logger, "\"request-dep\": (\"$dep\", \"$worker_addr\", $deps)")
        info(logger, "Request $(length(deps)) keys")

        try
            start_time = time()
            response = send_recv(
                worker.connection_pool,
                Address(worker_addr),
                Dict(
                    "op" => "get_data",
                    "reply" => true,
                    "keys" => [to_key(key) for key in deps],
                    "who" => worker.address,
                )
            )
            stop_time = time()

            response = Dict(k => to_deserialize(v) for (k,v) in response)
            if cause != ""
                push!(worker.startstops[cause], ("transfer", start_time, stop_time))
            end

            debug(logger, "\"receive-dep\": (\"$worker_addr\", $(collect(keys(response))))")

            if !isempty(response)
                send_msg(
                    get(worker.batched_stream),
                    Dict("op" => "add-keys", "keys" => collect(keys(response)))
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
                transition_dep(worker, dep, "memory", value=response[dep])
            elseif !haskey(worker.dep_state, dep) || worker.dep_state[dep] != "memory"
                transition_dep(worker, dep, "waiting", worker_addr=worker_addr)
            end

            if !haskey(response, dep) && haskey(worker.dependents, dep)
                debug(logger, "\"missing-dep\": \"$dep\"")
            end
        end

        if worker.validate
            validate_state(worker)
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
    @async begin
        if !isempty(deps)
            original_deps = deps
            debug(logger, "\"handle-missing\": $deps")

            deps = filter(dep -> haskey(worker.dependents, dep), deps)

            for dep in deps
                suspicious = worker.suspicious_deps[dep]
                if suspicious > 3
                    delete!(deps, dep)
                    bad_dep(worker, dep)
                end
            end

            if !isempty(deps)
                info(logger, "Dependents not found: $deps. Asking scheduler")

                who_has = send_recv(
                    worker.scheduler,
                    Dict("op" => "who_has", "keys" => [to_key(key) for key in deps])
                )
                who_has = Dict(k => v for (k,v) in filter((k,v) -> !isempty(v), who_has))
                update_who_has(worker, who_has)

                for dep in deps
                    worker.suspicious_deps[dep] += 1

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

                for dep in original_deps
                    delete!(worker.missing_dep_flight, dep)
                end

                ensure_communicating(worker)
            end
        end
    end
end

"""
    bad_dep(worker::Worker, dep::String)

Handle a bad dependency.
"""
function bad_dep(worker::Worker, dep::String)
    for key in worker.dependents[dep]
        msg = "Could not find dependent \"$dep\".  Check worker logs"
        worker.exceptions[key] = msg
        worker.tracebacks[key] = msg
        transition(worker, key, "error")
    end
    release_dep(worker, dep)
end

"""
    update_who_has(worker::Worker, who_has::Dict{String, String})

Ensure `who_has` is up to date and accurate.
"""
function update_who_has(worker::Worker, who_has::Dict{String, Array{Any, 1}})
    for (dep, workers) in who_has
        if !isempty(workers)
            continue
        end
        if haskey(worker.who_has, dep)
            push!(worker.who_has[dep], workers...)
        else
            worker.who_has[dep] = Set(workers)
        end

        for worker_address in workers
            push!(worker.has_what[worker], dep)
        end
    end
end

"""
    select_keys_for_gather(worker::Worker, worker_addr::String, dep::String)

Select which keys to gather from peer at `worker_addr`.
"""
function select_keys_for_gather(worker::Worker, worker_addr::String, dep::String)
    deps = Set([dep])

    total_bytes = worker.nbytes[dep]
    pending = worker.pending_data_per_worker[worker_addr]

    while !isempty(pending)
        dep = shift!(pending)
        if !haskey(worker.dep_state, dep) || worker.dep_state[dep] != "waiting"
            continue
        end
        if total_bytes + worker.nbytes[dep] > worker.target_message_size
            break
        end
        push!(deps, dep)
        total_bytes += worker.nbytes[dep]
    end

    return deps
end

"""
    gather_from_workers(worker::Worker, who_has::Dict)

Gather data directly from `who_has` peers.
"""
function gather_from_workers(who_has::Dict, connection_pool::ConnectionPool)
    bad_addresses = Set()
    missing_workers = Set()
    original_who_has = who_has
    who_has = Dict(k => Set(v) for (k,v) in who_has)
    results = Dict()
    all_bad_keys = Set()

    while length(results) + length(all_bad_keys) < length(who_has)
        directory = Dict{String, Array}()
        rev = Dict()
        bad_keys = Set()
        for (key, addresses) in who_has
            if haskey(results, key)
                continue
            elseif isempty(addresses)
                push!(all_bad_keys, key)
                continue
            end
            possible_addresses = collect(setdiff(addresses, bad_addresses))
            if isempty(possible_addresses)
                push!(all_bad_keys, key)
                continue
            end
            address = rand(possible_addresses)
            !haskey(directory, address) && (directory[address] = [])
            push!(directory[address], key)
            rev[key] = address
        end
        !isempty(bad_keys) && union!(all_bad_keys, bad_keys)

        responses = Dict()
        for (address, keys_to_gather) in directory
            try
                response = send_recv(
                    connection_pool,
                    address,
                    Dict(
                        "op" => "get_data",
                        "reply" => true,
                        "keys" => keys_to_gather,
                        "close" => false,
                    ),
                )
            catch exception
                warn(
                    logger,
                    "Worker stream died during communication \"$address\": " *
                    "$exception"
                )
                push!(missing_workers, address)
            finally
                responses[address] = response
            end
        end

        union!(bad_addresses, Set(v for (k, v) in rev if !haskey(responses, k)))
        merge!(results, responses)
    end

    bad_keys = Dict(k => collect(original_who_has[k]) for k in all_bad_keys)

    return results, bad_keys, collect(missing_workers)
end

##############################      TRANSITION FUNCTIONS      ##############################

"""
    transition(worker::Worker, key::String, finish_state::String; kwargs...)

Transition task with identifier `key` to finish_state from its current state.
"""
function transition(worker::Worker, key::String, finish_state::String; kwargs...)
     # Ensure the task hasn't been released (cancelled) by the scheduler
    if haskey(worker.tasks, key) && haskey(worker.task_state, key)
        start_state = worker.task_state[key]

        if start_state != finish_state
            transition_func = worker.transitions[start_state, finish_state]
            transition_func(worker, key, ;kwargs...)
            worker.task_state[key] = finish_state
        end
    end
end

function transition_waiting_ready(worker::Worker, key::String)
    if worker.validate
        @assert worker.task_state[key] == "waiting"
        @assert haskey(worker.waiting_for_data, key)
        @assert isempty(worker.waiting_for_data[key])
        @assert all(dep -> haskey(worker.data, dep), worker.dependencies[key])
        @assert key ∉ worker.executing
        @assert !haskey(worker.ready, key)
    end

    delete!(worker.waiting_for_data, key)
    enqueue!(worker.ready, key, worker.priorities[key])
end

function transition_waiting_done(worker::Worker, key::String; value::Any=nothing)
    if worker.validate
        @assert worker.task_state[key] == "waiting"
        @assert haskey(worker.waiting_for_data, key)
        @assert key ∉ worker.executing
        @assert !haskey(worker.ready, key)
    end

    delete!(worker.waiting_for_data, key)
    send_task_state_to_scheduler(worker, key)
end

function transition_ready_executing(worker::Worker, key::String)
    if worker.validate
        @assert !haskey(worker.waiting_for_data, key)
        @assert worker.task_state[key] == "ready"
        @assert !haskey(worker.ready, key)
        @assert all(dep -> haskey(worker.data, dep), worker.dependencies[key])
    end

    push!(worker.executing, key)
    execute(worker, key)
end

function transition_ready_memory(worker::Worker, key::String; value::Any=nothing)
    send_task_state_to_scheduler(worker, key)
end

function transition_executing_done(worker::Worker, key::String; value::Any=no_value)
    if worker.validate
        @assert key in worker.executing
        @assert !haskey(worker.waiting_for_data, key)
        @assert !haskey(worker.ready, key)
    end

    if worker.task_state[key] == "executing"
        delete!(worker.executing, key)
    end

    if value != no_value
        put_key_in_memory(worker, key, value, should_transition=false)
        if haskey(worker.dep_state, key)
            transition_dep(worker, key, "memory")
        end
    end

    send_task_state_to_scheduler(worker, key)
end

"""
    transition_dep(worker::Worker, dep::String, finish_state::String; kwargs...)

Transition dependency task with identifier `key` to finish_state from its current state.
"""
function transition_dep(worker::Worker, dep::String, finish_state::String; kwargs...)
    if haskey(worker.dep_state, dep)
        start_state = worker.dep_state[dep]

        if start_state != finish_state && !(start_state == "memory" && finish_state == "flight")
            func = worker.dep_transitions[(start_state, finish_state)]
            func(worker, dep, ;kwargs...)
            debug(logger, "\"$dep\": transition dependency $start_state => $finish_state")
        end
    end
end

function transition_dep_waiting_flight(worker::Worker, dep::String; worker_addr::String="")
    if worker.validate
        @assert worker_addr != ""
        @assert !haskey(worker.in_flight_tasks, dep)
        @assert haskey(worker.dependents, dep)
    end

    worker.in_flight_tasks[dep] = worker_addr
    worker.dep_state[dep] = "flight"
end

function transition_dep_flight_waiting(worker::Worker, dep::String; worker_addr::String="")
    if worker.validate
        @assert worker_addr != ""
        @assert haskey(worker.in_flight_tasks, dep)
    end

    delete!(worker.in_flight_tasks, dep)

    haskey(worker.who_has, dep) && delete!(worker.who_has[dep], worker_addr)
    haskey(worker.has_what, worker_addr) && delete!(worker.has_what[worker_addr], dep)

    if !haskey(worker.who_has, dep) || isempty(worker.who_has[dep])
        if dep ∉ worker.missing_dep_flight
            push!(worker.missing_dep_flight, dep)
            handle_missing_dep(worker, Set([dep]))
        end
    end

    for key in get(worker.dependents, dep, ())
        if worker.task_state[key] == "waiting"
            unshift!(worker.data_needed, key)
        end
    end

    if haskey(worker.dependents, dep) && isempty(worker.dependents[dep])
        release_dep(worker, dep)
    end
    worker.dep_state[dep] = "waiting"
end

function transition_dep_flight_memory(worker::Worker, dep::String; value=nothing)
    delete!(worker.in_flight_tasks, dep)
    worker.dep_state[dep] = "memory"
    put_key_in_memory(worker, dep, value)
end


function transition_dep_waiting_memory(worker::Worker, dep::String; value=nothing)
    worker.dep_state[dep] = "memory"
end


##############################      VALIDATION FUNCTIONS      ##############################

"""
    validate_key(worker::Worker, key::String)

Validate task with identifier `key`.
"""
function validate_key(worker::Worker, key::String)
    state = worker.task_state[key]
    if state == "memory"
        validate_key_memory(worker, key)
    elseif state == "waiting"
        validate_key_waiting(worker, key)
    elseif state == "ready"
        validate_key_ready(worker, key)
    elseif state == "executing"
        validate_key_executing(worker, key)
   end
end

function validate_key_memory(worker::Worker, key::String)
    @assert haskey(worker.data, key)
    @assert haskey(worker.nbytes, key)
    @assert !haskey(worker.waiting_for_data, key)
    @assert key ∉ worker.executing
    @assert !haskey(worker.ready, key)
    if haskey(worker.dep_state, key)
        @assert worker.dep_state[key] == "memory"
    end
end

function validate_key_executing(worker::Worker, key::String)
    @assert key in worker.executing
    @assert !haskey(worker.data, key)
    @assert !haskey(worker.waiting_for_data, key)
    @assert all(haskey(worker.data, dep) for dep in worker.dependencies[key])
end

function validate_key_ready(worker::Worker, key::String)
    @assert haskey(worker.ready, key)
    @assert !haskey(worker.data, key)
    @assert key ∉ worker.executing
    @assert !haskey(worker.waiting_for_data, key)
    @assert all(dep -> haskey(worker.data, dep), worker.dependencies[key])
end

function validate_key_waiting(worker::Worker, key::String)
    @assert !haskey(worker.data, key)
    @assert any(dep -> !haskey(worker.data, dep), worker.dependencies[key])
end

"""
    validate_dep(worker::Worker, dep::String)

Validate task dependency with identifier `key`.
"""
function validate_dep(worker::Worker, dep::String)
    state = worker.dep_state[dep]
    if state == "waiting"
        validate_dep_waiting(worker, dep)
    elseif state == "flight"
        validate_dep_flight(worker, dep)
    elseif state == "memory"
        validate_dep_memory(worker, dep)
    else
        error("Unknown dependent state: $state")
    end
end

function validate_dep_waiting(worker::Worker, dep::String)
    @assert !haskey(worker.data, dep)
    @assert haskey(worker.nbytes, dep)
    @assert !isempty(worker.dependents[dep])
    @assert !any(key -> haskey(worker.ready, key), worker.dependents[dep])
end

function validate_dep_flight(worker::Worker, dep::String)
    @assert !haskey(worker.data, dep)
    @assert haskey(worker.nbytes, dep)
    @assert !any(key -> haskey(worker.ready, key), worker.dependents[dep])
    peer = worker.in_flight_tasks[dep]
    @assert dep in worker.in_flight_workers[peer]
end

function validate_dep_memory(worker::Worker, dep::String)
    @assert haskey(worker.data, dep)
    @assert haskey(worker.nbytes, dep)
    @assert haskey(worker.types, dep)
    if haskey(worker.task_state, dep)
       @assert worker.task_state[dep] == "memory"
    end
end

"""
    validate_state(worker::Worker)

Validate current worker state.
"""
function validate_state(worker::Worker)
    if worker.status == "running"
        for (key, workers) in worker.who_has
            for worker_addr in workers
                @assert key in worker.has_what[worker_addr]
            end
        end

        for (worker_addr, keys) in worker.has_what
            for key in keys
                @assert worker_addr in worker.who_has[key]
            end
        end

        for (key, deps) in worker.waiting_for_data
            if key ∉ worker.data_needed
                for dep in deps
                    @assert (
                        haskey(worker.in_flight_tasks, dep) ||
                        dep in worker.missing_dep_flight ||
                        issubset(worker.who_has[dep], worker.in_flight_workers)
                    )
                end
            end
        end

        for key in keys(worker.tasks)
            if get(worker.task_state, key, nothing) == "memory"
                @assert isa(worker.nbytes[key], Integer)
                @assert !haskey(worker.waiting_for_data, key)
                @assert haskey(worker.data, key)
            end
        end
    end
end

##############################      SCHEDULER FUNCTIONS       ##############################

"""
    send_task_state_to_scheduler(worker::Worker, key::String)

Send the state of task `key` to the scheduler.
"""
function send_task_state_to_scheduler(worker::Worker, key::String)
    if haskey(worker.data, key)
        nbytes = get(worker.nbytes, key, sizeof(worker.data[key]))
        data_type = get(worker.types, key, typeof(worker.data[key]))

        msg = Dict{String, Any}(
            "op" => "task-finished",
            "status" => "OK",
            "key" => to_key(key),
            "nbytes" => nbytes,
            "type" => string(data_type)
        )
        if haskey(worker.startstops, key)
            msg["startstops"] = worker.startstops[key]
        end
        send_msg(get(worker.batched_stream), msg)

    elseif haskey(worker.exceptions, key)
        msg = Dict{String, Any}(
            "op" => "task-erred",
            "status" => "error",
            "key" => to_key(key),
            "exception" => worker.exceptions[key],
            "traceback" => worker.tracebacks[key],
        )
        if haskey(worker.startstops, key)
            msg["startstops"] = worker.startstops[key]
        end
        send_msg(get(worker.batched_stream), msg)
    end
end

##############################         OTHER FUNCTIONS        ##############################

"""
    deserialize_task(func, args, kwargs) -> Tuple

Deserialize task inputs and regularize to func, args, kwargs.

# Returns
- `Tuple`: The deserialized function, arguments and keyword arguments for the task.
"""
function deserialize_task(
    func::Union{String, Array},
    args::Union{String, Array},
    kwargs::Union{String, Array}
)
    !isempty(func) && (func = to_deserialize(func))
    !isempty(args) && (args = to_deserialize(args))
    !isempty(kwargs) && (kwargs = to_deserialize(kwargs))

    return (func, args, kwargs)
end

"""
    apply_function(func, args, kwargs) -> Dict()

Run a function and return collected information.
"""
function apply_function(func, args, kwargs)
    start_time = time()
    result_msg = Dict{String, Any}()
    try
        func = eval(func)
        result = func(args..., kwargs...)
        result_msg["op"] = "task-finished"
        result_msg["status"] = "OK"
        result_msg["result"] = result
        result_msg["nbytes"] = sizeof(result)
        result_msg["type"] = typeof(result)
    catch exception
        result_msg = Dict{String, Any}(
            "exception" => "$(typeof(exception))",
            "traceback" => sprint(showerror, exception),
            "op" => "task-erred"
        )
    end
    stop_time = time()
    result_msg["start"] = start_time
    result_msg["stop"] = stop_time
    return result_msg
end
