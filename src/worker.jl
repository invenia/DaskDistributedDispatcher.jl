# TODO: implement missing functionality, test EVERYTHING, cleanup, document code, etc.

const no_value = "--no-value-sentinel--"

const IN_PLAY = ("waiting", "ready", "executing", "long-running")
const PENDING = ("waiting", "ready", "constrained")
const PROCESSING = ("waiting", "ready", "constrained", "executing", "long-running")
const READY = ("ready", "constrained")

const PORT = 1024  # TODO: randomize ports instead of clustering around 1024

"""
    Worker

A `Worker` represents a worker endpoint in the distributed cluster that accepts instructions
from the scheduler, fetches dependencies, executes compuations, stores data, and
communicates state to the scheduler.
"""
type Worker
    # TODO: delete elements not used, reorganize as makes sense and document
    # the purpose of each variable unless obvious

    # Communication management
    scheduler_address::URI
    host::IPAddr
    port::Integer
    listener::Base.TCPServer
    sock::TCPSocket
    batched_stream::Nullable{BatchedSend}
    scheduler::Rpc
    handlers::Dict{String, Function}

    # Data and resource management
    available_resources::Dict  # TODO: what kind of dict?
    data::Dict{String, Any}  # maps keys to the results of function calls
    futures::Dict{String, DeferredFutures.DeferredFuture}
    priorities::Dict{String, Tuple}
    nbytes::Dict{String, Integer}
    types::Dict{String, Type}
    who_has::Dict{String, Set}
    has_what::DefaultDict{String, Set}

    # Task management
    tasks::Dict{String, Tuple}
    task_state::Dict{String, String}

    # Task state management
    transitions::Dict{Tuple{String, String}, Function}
    ready::PriorityQueue{String, Tuple, Base.Order.ForwardOrdering}
    constrained::Deque{String}
    data_needed::Deque{String}
    executing::Set{String}
    long_running::Set{String}

    # Dependency management
    dep_state::Dict{String, String}
    dependencies::Dict{String, Set}
    dependents::Dict{String, Set}
    waiting_for_data::Dict{String, Set}
    pending_data_per_worker::DefaultDict{String, Deque{String}}
    resource_restrictions::Dict{String, Dict}
    in_flight_tasks::Dict
    in_flight_workers::Dict  # TODO: is this needed? what kind of dict?

    # Logging information
    is_computing::Bool
    status::String
    executed_count::Integer
    log::Deque{Tuple}
    exceptions::Dict{String, String}  # TODO: where all should this be used?
    tracebacks::Dict{String, String}  # TODO: where all should this be used?
    durations::Dict  # TODO: what kind of dict?
    startstops::DefaultDict{String, Array}

    # Validation
    validate::Bool
end

"""
    Worker(scheduler_address::String)

Creates a `Worker` that listens on a random port for incoming messages.
"""
function Worker(scheduler_address::String)
    port = rand(1024:9000)
    scheduler_address = build_URI(scheduler_address)

    # This is the minimal set of handlers needed
    # https://github.com/JuliaParallel/Dagger.jl/issues/53
    handlers = Dict{String, Function}(
        "compute-stream" => compute_stream,
        "get_data" => get_data,
        "gather" => gather,
        "delete_data" => delete_data,
        "terminate" => terminate,
        "keys" => get_keys,
    )
    transitions = Dict{Tuple{String, String}, Function}(
        ("waiting", "ready") => transition_waiting_ready,
        ("waiting", "memory") => transition_waiting_memory,
        ("ready", "executing") => transition_ready_executing,
        ("ready", "memory") => transition_ready_memory,
        ("constrained", "executing") => transition_constrained_executing,
        ("executing", "memory") => transition_executing_done,
        ("executing", "error") => transition_executing_done,
        ("executing", "long-running") => transition_executing_long_running,
        ("long-running", "error") => transition_executing_done,
        ("long-running", "memory") => transition_executing_done,
    )
    worker = Worker(
        scheduler_address,
        getipaddr(),  # host
        listenany(port)...,  # port and listener
        TCPSocket(), # sock placeholder? TODO: see if this can be reused when initialized properly
        nothing, #  batched_stream
        Rpc(scheduler_address),  # scheduler
        handlers,

        Dict(),  # available_resources
        Dict{String, Any}(),  # data
        Dict{String, DeferredFutures.DeferredFuture}(), # futures
        Dict{String, Tuple}(),  # priorities
        Dict{String, Integer}(),  # nbytes
        Dict{String, Type}(),  # types
        Dict{String, Set}(),  # who_has
        DefaultDict{String, Set}(Set()),  # has_what

        Dict{String, Tuple}(),  # tasks
        Dict{String, String}(),  #task_state

        transitions,  # transitions
        PriorityQueue(String, Tuple, Base.Order.ForwardOrdering()), # ready
        Deque{String}(),  # constrained
        Deque{String}(),  # data_needed
        Set{String}(),  # executing
        Set{String}(),  # long_running

        Dict{String, String}(),  # dep_state
        Dict{String, Set}(),  # dependencies
        Dict{String, Set}(),  # dependents
        Dict{String, Set}(),  # waiting_for_data
        DefaultDict{String, Deque{String}}(Deque{String}()),  # pending_data_per_worker
        Dict{String, Dict}(),  # resource_restrictions
        Dict(),  # in_flight_tasks
        Dict(),  # in_flight_workers

        false,  # is_computing
        "starting",  # status
        0,
        Deque{Tuple}(),  # log
        Dict{String, String}(),  # exceptions
        Dict{String, String}(),  # tracebacks
        Dict(),  # durations
        DefaultDict{String, Array}([]),  # startstops

        true,  # validation
    )

    start_worker(worker)
    return worker
end

##############################       ADMIN FUNCTIONS        ##############################

"""
    address(worker::Worker)

Returns this Workers's address formatted as an URI.
"""
address(worker::Worker) = return string(build_URI(worker.host, worker.port))

"""
    show(io::IO, worker::Worker)

Prints a representation of the worker and it's state.
"""
function Base.show(io::IO, worker::Worker)
    @printf(
        io,
        "<%s: %s, %s, stored: %d, running: %d, ready: %d, comm: %d, waiting: %d>",
        typeof(worker).name.name, address(worker), worker.status,
        length(worker.data), length(worker.executing),
        length(worker.ready), length(worker.in_flight_tasks),
        length(worker.waiting_for_data),
    )
end

"""
    start_worker(worker::Worker)

Coordinates a worker's startup.
"""
function start_worker(worker::Worker)
    @assert worker.status == "starting"

    start_listening(worker)

    info(logger, "      Start worker at: $(address(worker))")
    info(logger, "Waiting to connect to: $(worker.scheduler_address)")
    info(logger, string("-" ^ 49))

    register_worker(worker)
end

"""
    register_worker(worker::Worker)

Registers a `Worker` with the dask-scheduler process.
"""
function register_worker(worker::Worker)
    @async begin
        response = send_recv(
            worker.scheduler,
            Dict(
                "op" => "register",
                "address" => chop(address(worker)),
                "ncores" => Sys.CPU_CORES,
                "keys" => collect(keys(worker.data)),
                "nbytes" => worker.nbytes,
                "now" => time(),
                "executing" => length(worker.executing),
                "in_memory" => length(worker.data),
                "ready" => length(worker.ready),
                "in_flight" => length(worker.in_flight_tasks),
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
    start_listening(worker::Worker)

Listens for incoming messages on a random port initialized on startup.
"""
function start_listening(worker::Worker)
    @async while isopen(worker.listener)
        worker.sock = accept(worker.listener)
        @async while isopen(worker.listener) && isopen(worker.sock)
            try
                msgs = recv_msg(worker.sock)
                incoming_host, incoming_port = getsockname(worker.sock)
                incoming_address = build_URI(incoming_host, incoming_port)
                debug(logger, "Message received from $incoming_address: \n$msgs")

                if isa(msgs, Array)
                    for msg in msgs
                        @async handle_incoming_msg(worker, msg)
                    end
                else
                    @async handle_incoming_msg(worker, msgs)
                end

                if worker.is_computing
                    ensure_computing(worker)
                end
            catch exception
                # EOFErrors are expected when connections are closed unexpectadly
                isa(exception, EOFError) || rethrow(exception)
            end
        end
    end
end

"""
    Base.close(worker::Worker)

Closes the worker and all the connections it has open.
"""
function Base.close(worker::Worker)
    # TODO: implement shutdown, this has only been started

    if worker.status ∉ ("closed", "closing")
        info(logger, "Stopping worker at $(address(worker))")
        worker.status = "closing"
        # stop(worker)
        # self.heartbeat_callback.stop()
        # with ignoring(EnvironmentError, gen.TimeoutError):
        # if report  # TODO: implement reporting maybe
        #     scheduler.unregister(address=self.address)
        # end
        # self.scheduler.close_rpc()
        # self.executor.shutdown()
        # if os.path.exists(self.local_dir):  # TODO: do we need files
        #     shutil.rmtree(self.local_dir)

        # for k, v in self.services.items():
        #     v.stop()

        worker.status = "closed"

        # if nanny and 'nanny' in self.services:
        #     with self.rpc(self.services['nanny']) as r:
        #         yield r.terminate()

        # self.rpc.close()
        Base.close(worker.scheduler)
        Base.close(worker.batched_stream)
        Base.close(worker.listener)
        # self._closed.set()
    end
end

"""
    handle_incoming_msg(worker::Worker, msg::Dict)

Handle message received by the worker.
"""
function handle_incoming_msg(worker::Worker, msg::Dict)
    op = pop!(msg, "op", nothing)
    reply = pop!(msg, "reply", nothing)
    terminate = pop!(msg, "close", nothing)  # Figure out what to do with close

    haskey(msg, "key") && validate_key(msg["key"])
    msg = Dict(parse(k) => v for (k,v) in msg)

    # TODO: refactor
    if worker.is_computing && !haskey(worker.handlers, op)
        if op == "close"
            closed = true  # TODO: what is this used for
            close(worker)
            return
        elseif op == "compute-task"
            add_task(worker, ;msg...)
        elseif op == "release-task"
            push!(worker.log, (msg[:key], "release-task"))
            release_key(worker, ;msg...)
        elseif op == "delete-data"
            delete_data(worker, ;msg...)
        else
            warn(logger, "Unknown operation $op, $msg")
        end
        ensure_computing(worker)
    else
        try
            handler = worker.handlers[op]
            handler(worker, ;msg...)

        catch exception
            error("No handler found for $op: $exception")
        end
    end
end

##############################       HANDLER FUNCTIONS        ##############################

"""
    compute_stream(worker::Worker)

Set is_computing to true so that the worker can manage state.
"""
function compute_stream(worker::Worker)
    worker.is_computing = true
    worker.batched_stream = BatchedSend(worker.sock)
end

"""
    get_data(worker::Worker; keys::Array=[])

Sends the results of `keys` to the scheduler.
"""
function get_data(worker::Worker; keys::Array=[])
    # TODO: is this all that is needed? Update once future stuff has been figured out
    notice(logger, "in get_data")
    @async begin
        data = Dict(
            k =>
            to_serialize(worker.data[k]) for k in filter(k -> haskey(worker.data, k), keys)
        )
        # nbytes = Dict(
        #     k => worker.nbytes[k] for k in filter(k -> haskey(worker.data, k), keys)
        # )
        send_msg(worker.sock, data)
    end
end

"""
    gather(worker::Worker)

Gathers the results for various keys.
"""
function gather(worker::Worker)
    warn(logger, "Not implemented `gather` yet")
end

"""
    delete_data(worker::Worker; keys::Array=[], report::String="true")

Deletes the data associated with each key of `keys` in `worker.data`.
"""
function delete_data(worker::Worker; keys::Array=[], report::String="true")
    @async begin
        for key in keys
            debug(logger, "delete key: $key")
            haskey(worker.task_state, key) && release_key(worker, key=key)
            haskey(worker.dep_state, key) && release_dep(worker, key)
        end

        debug(logger, "Deleted $(length(keys)) keys")
        if report == "true"
            debug(logger, "Reporting loss of keys to scheduler")
            msg = Dict(
                "op" => "remove-keys",
                "address" => address(worker),
                "keys" => [to_key(key) for key in keys],
            )
            send_msg(worker.scheduler, msg)
        end
    end
end

function terminate(worker::Worker, msg::Dict)
    warn(logger, "Not implemented `terminate` yet")
end

function get_keys(worker::Worker, msg::Dict)
    return keys(worker.data)  # TODO: return array instead of iterator maybe?
end


############################## COMPUTE-STREAM HELPER FUNCTIONS #############################

function add_task(
    worker::Worker;
    key::String="",
    priority::Array=[],
    duration=nothing,
    who_has=nothing,
    nbytes=nothing,
    resource_restrictions=nothing,
    func=nothing,
    args=nothing,
    kwargs=nothing,
    future=nothing,
)
    if key == "" || priority == []
        throw(ArgumentError("Key or task priority cannot be empty"))
    end
    if haskey(worker.tasks, key)
        state = worker.task_state[key]
        if state in ("memory", "error")
            if state == "memory"
                @assert key in worker.data
            end
            debug(logger, "Asked to compute pre-existing result: $key: $state")
            send_task_state_to_scheduler(worker, key)
            return
        end
        if state in IN_PLAY
            return
        end
    end

    if haskey(worker.dep_state, key) && worker.dep_state[key] == "memory"
        worker.task_state[key] = "memory"
        send_task_state_to_scheduler(worker, key)
        worker.tasks[key] = nothing
        push!(worker.log, (key, "new-task-already-in-memory"))  # TODO: verify all log
        worker.priorities[key] = tuple(map(parse, priority)...)
        worker.durations[key] = duration
        return
    end

    push!(worker.log, (key, "new"))
    try
        start = time()
        worker.tasks[key] = deserialize_task(func, args, kwargs)
        debug(logger, "In add_task: $(worker.tasks[key])")
        stop = time()

        if stop - start > 0.010
            push!(worker.startstops[key], ("deserialize", start, stop))
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
            "Could not deserialize task with key: \"$key\": $(error_msg["traceback"])"
        )
        send_msg(get(worker.batched_stream), error_msg)
        push!(worker.log, (key, "deserialize-error"))
        return
    end

    if future != nothing
        worker.futures[key] = to_deserialize(future)
    end

    worker.priorities[key] = tuple(map(parse, priority)...)
    worker.durations[key] = duration
    if resource_restrictions != nothing
        worker.resource_restrictions[key] = resource_restrictions
    end
    worker.task_state[key] = "waiting"

    if nbytes != nothing
        for (k,v) in nbytes
            worker.nbytes[k] = parse(v)
        end
    end

    who_has = who_has != nothing ? who_has : Dict()
    worker.dependencies[key] = Set(keys(who_has))
    worker.waiting_for_data[key] = Set()

    for dep in keys(who_has)
        if !haskey(worker.dependents, dep)
            worker.dependents[dep] = Set()
        end
        push!(worker.dependents[dep], key)

        if !haskey(worker.dep_state, dep)
            if worker.task_state[dep] == "memory"
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
        @assert workers != nothing
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
        for dep in keys(who_has)
            validate_dep(worker, dep)
        end
        validate_key(worker, key)
    end
end

function release_key(worker::Worker; key::String="", cause=nothing, reason::String="")
    notice(logger, "in release_key")
    if key == "" || !haskey(worker.task_state, key)
        return
    end

    state = pop!(worker.task_state, key)
    if reason == "stolen" && state in ("executing", "long-running", "memory")
        worker.task_state[key] = state
        return
    end

    if cause != nothing
        push!(worker.log, (key, "release-key", cause))
    else
        push!(worker.log, (key, "release-key"))
    end

    delete!(worker.tasks, key)
    if haskey(worker.data, key) && !haskey(worker.dep_state, key)
        delete!(worker.data, key)
        delete!(worker.nbytes, key)
        delete!(worker.types, key)
    end

    haskey(worker.waiting_for_data, key) && delete!(worker.waiting_for_data, key)

    for dep in pop!(worker.dependencies, key, ())
        delete!(worker.dependents[dep], key)
        if worker.dependents[dep] == nothing && worker.dep_state[dep] == "waiting"
            release_dep(worker, dep)
        end
    end

    delete!(worker.priorities, key)
    delete!(worker.durations, key)

    haskey(worker.exceptions, key) && delete!(worker.exceptions, key)
    haskey(worker.tracebacks, key) && delete!(worker.tracebacks, key)
    haskey(worker.startstops, key) && delete!(worker.startstops, key)

    if key in worker.executing
        delete!(worker.executing, key)
    end

    haskey(worker.resource_restrictions, key) && delete!(worker.resource_restrictions, key)

    if state in PROCESSING  # not finished
        send_msg(
            get(worker.batched_stream),
            Dict("op" => "release", "key" => to_key(key), "cause" => cause)
        )
    end
end

function release_dep(worker::Worker, dep::String)
    if haskey(worker.dep_state, dep)
        push!(worker.log, (dep, "release-dep"))
        pop!(worker.dep_state, dep)

        # if dep in worker.suspicious_deps
        #     del worker.suspicious_deps[dep]
        # end

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
            if worker.task_state[key] != "memory"
                release_key(worker, key, cause=dep)
            end
        end
    end
end

##############################       EXECUTING FUNCTIONS      ##############################

function meets_resource_requirements(worker::Worker, key::String)
    if haskey(worker.resource_restrictions, key) == false
        return true
    end
    for (resource, needed) in worker.resource_restrictions[key]
        warn(logger, "Actually used available resources")
        if worker.available_resources[resource] < needed
            return false
        end
    end

    return true
end

function ensure_computing(worker::Worker)
    while !isempty(worker.constrained)  # TODO: Add isbusy variable?
        notice(logger, "in ensure_computing: processing constrained")
        key = worker.constrained[1]
        if worker.task_state[key] != "constained"
            shift!(worker.constrained)
            continue
        end
        if meets_resource_requirements(worker, key)
            shift!(worker.constrained)
            transition(worker, key, "executing")
        else
            break
        end
    end
    while !isempty(worker.ready)  # TODO: Add isbusy variable?
        notice(logger, "in ensure_computing: processing ready")
        key = dequeue!(worker.ready)
        if worker.task_state[key] in READY
            transition(worker, key, "executing")
        end
    end
end

function execute(worker::Worker, key::String, report=false)
    @async begin
        notice(logger, "executing")
        # try
        if key ∉ worker.executing || !haskey(worker.task_state, key)
            return
        end
        if worker.validate
            @assert !haskey(worker.waiting_for_data, key)
            @assert worker.task_state[key] == "executing"
        end

        (func, args, kwargs) = worker.tasks[key]

        start = time()
        args2 = pack_data(args, worker.data, key_types=String)
        kwargs2 = pack_data(kwargs, worker.data, key_types=String)
        stop = time()
        if stop - start > 0.005
            push!(worker.startstops[key], ("disk-read", start, stop))
        end

        debug(logger, "Execute key $key worker $(address(worker))")
        result = apply_function(func, args2, kwargs2)
        debug(logger, "Executed $((func, args2, kwargs2)) and the result was: $result")

        if worker.task_state[key] ∉ ("executing", "long-running")
            return
        end

        result["key"] = key
        value = pop!(result, "result", nothing)

        push!(worker.startstops[key], ("compute", result["start"], result["stop"]))

        if result["op"] == "task-finished"
            !isready(worker.futures[key]) && put!(worker.futures[key], value)
            worker.nbytes[key] = result["nbytes"]
            worker.types[key] = result["type"]
            transition(worker, key, "memory", value=value)
        else
            !isready(worker.futures[key]) && put!(worker.futures[key], result["exception"])
            worker.exceptions[key] = result["exception"]
            worker.tracebacks[key] = result["traceback"]
            warn(
                logger,
                "Compute Failed\n" *
                "Function:  $func\n" *
                "args:      $args\n" *
                "kwargs:    $kwargs\n" *
                "Exception: $(result["exception"])\n" *
                "Traceback: $(result["traceback"])"
            )
            transition(worker, key, "error")
        end

        debug(logger, "Send compute response to scheduler: $key, $result")

        if worker.validate
            @assert key ∉ worker.executing
            @assert !haskey(worker.waiting_for_data, key)
        end

        ensure_computing(worker)
            # ensure_communicating(worker)  # TODO: implement

        if key in worker.executing
            delete!(worker.executing, key)
        end
    end
end

##############################      TRANSITION FUNCTIONS      ##############################

function transition(worker::Worker, key::String, to_state::String; kwargs...)
    notice(logger, "In transition: transitioning key $key to $to_state")
    from_state = worker.task_state[key]

    if from_state == to_state
        warn(logger, "Called `transition` with same start and end state")
        return
    end

    transition_func = worker.transitions[from_state, to_state]
    new_state = transition_func(worker, key, ;kwargs...)

    worker.task_state[key] = new_state

end

function transition_waiting_ready(worker::Worker, key::String)
    notice(logger, "in transition_waiting_ready")
    if worker.validate
        @assert worker.task_state[key] == "waiting"
        @assert haskey(worker.waiting_for_data, key)
        @assert isempty(worker.waiting_for_data[key])
        @assert all(dep -> haskey(worker.data, dep), worker.dependencies[key])
        @assert key ∉ worker.executing
        @assert !haskey(worker.ready, key)
    end

    delete!(worker.waiting_for_data, key)
    if haskey(worker.resource_restrictions, key)
        push!(worker.constrained, key)
        return "constrained"
    else
        enqueue!(worker.ready, key, worker.priorities[key])
        return "ready"
    end
end

function transition_waiting_memory(worker::Worker, key::String, value::Any=nothing)
    notice(logger, "in transition_waiting_memory")
    if worker.validate
        @assert worker.task_state[key] == "waiting"
        @assert key in worker.waiting_for_data
        @assert key ∉ worker.executing
        @assert !haskey(worker.ready, key)
    end

    delete!(worker.waiting_for_data, key)
    send_task_state_to_scheduler(worker, key)
    return "memory"
end

function transition_ready_executing(worker::Worker, key::String)
    notice(logger, "in transition_ready_executing")
    if worker.validate
        @assert !haskey(worker.waiting_for_data, key)
        @assert worker.task_state[key] in READY
        @assert !haskey(worker.ready, key)
        @assert all(dep -> haskey(worker.data, dep), worker.dependencies[key])
    end

    push!(worker.executing, key)
    execute(worker, key)  # Does this need to be @async?
    return "executing"
end

function transition_ready_memory(worker::Worker, key::String, value::Any=nothing)
    notice(logger, "in transition_ready_memory")
    send_task_state_to_scheduler(worker, key)
    notice(logger, "done transition_ready_memory")
    return "memory"
end

function transition_constrained_executing(worker::Worker, key::String)
    notice(logger, "in transition_constrained_executing")
    transition_ready_executing(worker, key)
    for (resource, quantity) in worker.resource_restrictions[key]
        worker.available_resources[resource] -= quantity
    end

    if worker.validate
        @assert all(v >= 0 for v in values(worker.available_resources))
    end
    return "executing"
end

function transition_executing_done(worker::Worker, key::String; value::Any=no_value)
    notice(logger, "in transition_executing_done")
    if worker.validate
        @assert key in worker.executing || key in worker.long_running
        @assert !haskey(worker.waiting_for_data, key)
        @assert !haskey(worker.ready, key)
    end

    if haskey(worker.resource_restrictions, key)
        for (resource, quantity) in worker.resource_restrictions[key]
            worker.available_resources[resource] += quantity
        end
    end

    if worker.task_state[key] == "executing"
        delete!(worker.executing, key)
        worker.executed_count += 1
    elseif worker.task_state[key] == "long-running"
        delete!(worker.long_running, key)
    end

    if value != no_value
        put_key_in_memory(worker, key, value, transition=false)
        if haskey(worker.dep_state, key)
            transition_dep(worker, key, "memory")
        end
    end

    if !isnull(worker.batched_stream)
        send_task_state_to_scheduler(worker, key)
    else
        error("Connection closed in transition_executing_done")  # TODO: throw better error
    end

    notice(logger, "done transition_executing_done")

    return "memory"
end

function transition_executing_long_running(worker::Worker, key::String)
    notice(logger, "in transition_executing_long_running")
    if worker.validate
        @assert key in worker.executing
    end

    delete!(worker.executing, key)
    push!(worker.long_running, key)
    send_msg(get(worker.batched_stream), Dict("op" => "long-running", "key" => key))

    ensure_computing(worker)
    return "long-running"
end

# TODO: rename/move section
##############################      TRANSITION HELPERS?       ##############################

function put_key_in_memory(worker::Worker, key::String, value::Any; transition::Bool=true)
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

        if transition && haskey(worker.task_state, key)
            transition(worker, key, "memory")
        end

        push!(worker.log, (key, "put-in-memory"))
    end
end


##############################      VALIDATION FUNCTIONS      ##############################

function validate_key_memory(worker::Worker, key::String)
    @assert haskey(worker.data, key)
    @assert haskey(worker.nbytes, key)
    @assert !haskey(worker.waiting_for_data, key)
    @assert !haskey(worker.executing, key)
    @assert !haskey(worker.ready, key)
    if key in worker.dep_state
        @assert worker.dep_state[key] == "memory"
    end
end

function validate_key_executing(worker::Worker, key::String)
    @assert key in worker.executing
    @assert !haskey(worker.data, key)
    @assert !haskey(worker.waiting_for_data, key)
    @assert all(dep in worker.data for dep in worker.dependencies[key])
end

function validate_key_ready(worker::Worker, key::String)
    @assert key in peek(worker.ready)
    @assert !haskey(worker.data, key)
    @assert key ∉ worker.executing
    @assert !haskey(worker.waiting_for_data, key)
    @assert all(dep -> haskey(worker.data, dep), worker.dependencies[key])
end

function validate_key_waiting(worker::Worker, key::String)
    @assert !haskey(worker.data, key)
    notice(logger, "$(worker.dependencies[key])")
    notice(logger, "$(any(dep -> dep ∉ worker.data, worker.dependencies[key]))")
    @assert any(dep -> dep ∉ worker.data, worker.dependencies[key])
end

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

function validate_dep_waiting(worker::Worker, dep::String)
    @assert !haskey(worker.data, dep)
    @assert haskey(worker.nbytes, dep)
    @assert worker.dependents[dep] # ???
    @assert !any(key -> haskey(worker.ready, key), worker.dependents[dep])
end

function validate_dep_flight(worker::Worker, dep::String)
    @assert !haskey(worker.data, dep)
    @assert dep in worker.nbytes
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

function validate_state(worker::Worker, key::String)
    if worker.status != "running"
       return
   end
    for (key, workers) in worker.who_has
        for worker in workers
            @assert key in worker.has_what[worker]
        end
    end

    for (worker, keys) in worker.has_what
        for key in keys
            @assert worker in worker.who_has[key]
        end
    end

    for key in worker.task_state
        worker.validate_key(key)
    end

    for dep in worker.dep_state
        worker.validate_dep(dep)
    end

    for (key, deps) in worker.waiting_for_data
        if !haskey(worker.data_needed, key)
            for dep in deps
                @assert (
                    dep in worker.in_flight_tasks ||
                    dep in worker._missing_dep_flight ||
                    worker.who_has[dep].issubset(worker.in_flight_workers)
                )
            end
        end
    end

    for key in worker.tasks
        if worker.task_state[key] == "memory"
            @assert isinstance(worker.nbytes[key], int)
            @assert !haskey(worker.waiting_for_data, key)
            @assert key in worker.data
        end
    end
end

##############################      SCHEDULER FUNCTIONS       ##############################

function send_task_state_to_scheduler(worker::Worker, key::String)
    if haskey(worker.data, key)
        nbytes = get(worker.nbytes, key, sizeof(worker.data[key]))
        oftype = get(worker.types, key, typeof(worker.data[key]))

        msg = Dict{String, Any}(
            "op" => "task-finished",
            "status" => "OK",
            "key" => to_key(key),
            "nbytes" => nbytes,
            "type" => string(oftype)
        )
    elseif haskey(worker.exceptions, key)
        msg = Dict{String, Any}(
            "op" => "task-erred",
            "status" => "error",
            "key" => to_key(key),
            "exception" => worker.exceptions[key],
            "traceback" => worker.tracebacks[key],
        )
    else
        error(logger, "Key not ready to send to worker, $key: $(worker.task_state[key])")
        return
    end

    if haskey(worker.startstops, key)
        msg["startstops"] = worker.startstops[key]
    end

    op = Dict{String, Any}("op" => "task-erred")
    headers = Dict(
        "headers" => [
            "compression" => nothing,
            lengths = [40],
        ],
        "keys" => [("status",)],
    )

    send_msg(get(worker.batched_stream), msg)
end

##############################         OTHER FUNCTIONS        ##############################

"""
    deserialize_task(func, args, kwargs) -> (func, args, kwargs)

Deserialize task inputs and regularize to func, args, kwargs.
"""
function deserialize_task(func, args, kwargs)
    func != nothing && (func = to_deserialize(func))
    args != nothing && (args = to_deserialize(args))
    kwargs != nothing && (kwargs = to_deserialize(kwargs))

    notice(logger, "in deserialize_task: the output is: $((func, args, kwargs))")

    return (func, args, kwargs)
end

"""
    apply_function(func, args, kwargs) -> Dict()

Run a function and return collected information.
"""
function apply_function(func, args, kwargs)
    start = time()
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
    stop = time()
    result_msg["start"] = start
    result_msg["stop"] = stop
    return result_msg
end
