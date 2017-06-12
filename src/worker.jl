# TODO: implement missing functionality, test EVERYTHING, cleanup, document code, etc.


const IN_PLAY = ("waiting", "ready", "executing", "long-running")
const PENDING = ("waiting", "ready", "constrained")
const PROCESSING = ("waiting", "ready", "constrained", "executing", "long-running")
const READY = ("ready", "constrained")

const PORT = 1024  # TODO: randomize ports instead of clustering around 1024

"""
`Worker` represents a worker endpoint in the distributed cluster
"""
type Worker
    # TODO: delete elements not used, reorganize as makes sense and document
    # the purpose of each variable unless obvious
    port_to_listen_on::Integer
    host::IPAddr
    # address::URI
    port::Integer
    listener::Any
    sock::TCPSocket
    ##### WORKER BASED HERE

    # self._port = 0
    # self.ncores = ncores or _ncores
    # self.local_dir = local_dir or tempfile.mkdtemp(prefix='worker-')
    # self.total_resources = resources or {}
    available_resources::Dict  # TODO: what kind of dict?
    # self.death_timeout = death_timeout
    # self.preload = preload
    # self.memory_limit = memory_limit

    data::Dict{String, Any}  # maps keys to the results of function calls  #TODO: hold remote refs/results

    # self.loop = loop or IOLoop.current()
    comm::TCPSocket  # TODO: use RPC instead?
    # self.status = None
    status::String
    # self._closed = Event()
    # self.reconnect = reconnect
    # self.executor = executor or ThreadPoolExecutor(self.ncores)
    # self.scheduler = rpc(scheduler_addr)
    scheduler_address::URI
    # self.name = name
    # self.heartbeat_interval = heartbeat_interval
    # self.heartbeat_active = False
    # self.execution_state = {'scheduler': self.scheduler.address,
    #                         'ioloop': self.loop,
    #                         'worker': self}
    # self._last_disk_io = None
    # self._last_net_io = None
    # self._ipython_kernel = None
    #
    # if self.local_dir not in sys.path:
    #     sys.path.insert(0, self.local_dir)
    #
    # self.services = {}
    # self.service_ports = service_ports or {}
    # self.service_specs = services or {}

    handlers::Dict{String, Function}


    ##### WORKER STARTS HERE

    # Task management
    tasks::Dict{String, Tuple}
    task_state::Dict{String, String}
    dep_state::Dict  # TODO: is this needed? what kind of dict?
    dependencies::Dict{String, Set}
    dependents::Dict{String, Set}
    waiting_for_data::Dict{String, Set}
    who_has::Dict{String, Set}  # TODO: what kind of dict?
    has_what::Dict  # TODO: what kind of dict?
    pending_data_per_worker::Dict  # TODO: what kind of dict? original: defaultdict(deque)
    extensions::Dict  # TODO: is this needed? what kind of dict?

    data_needed::Array  # TODO:  what type? original: deque() or heap

    # Memory management ?
    in_flight_tasks::Dict
    in_flight_workers::Dict  # TODO: is this needed? what kind of dict?
    # self.total_connections = 50
    # self.total_comm_nbytes = 10e6
    # self.comm_nbytes = 0
    # self.suspicious_deps = defaultdict(lambda: 0)
    # self._missing_dep_flight = set()
    #
    # Cache information to avoid recomputation
    nbytes::Dict  # TODO: what kind of dict?
    types::Dict  # TODO: what kind of dict?
    # self.threads = dict()
    exceptions::Dict{String, String}  # TODO: where all should this be used?
    tracebacks::Dict{String, String}  # TODO: where all should this be used?
    #
    # Resource management ? is this needed only for threading
    priorities::Dict  # TODO: what kind of dict?
    # self.priority_counter = 0
    durations::Dict  # TODO: what kind of dict?
    startstops::Dict{String, Array}  # TODO: does this really need to be an array or is a tuple fine?
    resource_restrictions::Dict{String, Dict}  # TODO: what kind of dict?
    #
    # Task state management
    transitions::Dict{Tuple{String, String}, Function}
    ready::Base.Collections.PriorityQueue{String, Integer, Base.Base.Order.Ordering}
    constrained::Array  # TODO: what kind of array?
    executing::Set  #TODO: what kind of set?
    executed_count::Integer
    long_running::Set
    #
    # Message management
    # self.batched_stream = None
    # self.recent_messages_log = deque(maxlen=10000)
    # self.target_message_size = 50e6  # 50 MB
    #
    # Logging management
    # FIXME why is log coloured?
    log::Array
    validate::Bool
    #
    # self.incoming_transfer_log = deque(maxlen=(100000))
    # self.incoming_count = 0
    # self.outgoing_transfer_log = deque(maxlen=(100000))
    # self.outgoing_count = 0
end

# TODO: Update documentation throughout file (and really everywhere lol)
"""
    Worker(address::String) -> Worker

Creates a `Worker` type that listens on `host` and `port` for messages.
"""
function Worker(scheduler_address::String, port_to_listen_on::Integer=PORT)
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
        port_to_listen_on,
        getipaddr(),
        listenany(port_to_listen_on)...,
        TCPSocket(), # placeholder? TODO: see if this can be reused when initialized properly
        Dict(),
        Dict{String, Any}(),
        connect(TCPSocket(), scheduler_address.host, scheduler_address.port),
        "starting",
        scheduler_address,
        handlers,
        Dict{String, Tuple}(),
        Dict{String, String}(),
        Dict(),
        Dict{String, Set}(),
        Dict{String, Set}(),
        Dict{String, Set}(),
        Dict{String, Set}(),
        Dict(),
        Dict(),
        Dict(),
        [],
        Dict(),
        Dict(),
        Dict(),
        Dict(),
        Dict{String, String}(),
        Dict{String, String}(),
        Dict(),
        Dict(),
        Dict{String, Array}(),
        Dict{String, Dict}(),
        transitions,
        Base.Collections.PriorityQueue{String, Integer, Base.Base.Order.Ordering}(),
        [],
        Set(),
        0,
        Set(),
        [],
        true,
    )

    sizehint!(worker.log, 100000)
    start_worker(worker)
    return worker
end

##############################       ADMIN FUNCTIONS        ##############################

"""
Returns this Workers's address formatted as an URI.
"""
# TODO: should I just store instead of computing this?
address(worker::Worker) = return string(build_URI(worker.host, worker.port))  # how often is this needed

function Base.show(io::IO, worker::Worker)
    @printf(
        io,
        "<%s: %s, %s, stored: %d, running: %d, ready: %d, comm: %d, waiting: %d>",
        typeof(worker).name.name, address(worker), worker.status,
        length(worker.data), length(worker.executing),
        length(worker.ready), length(worker.in_flight_tasks),
        length(worker.waiting_for_data)
    )
end

function start_worker(worker::Worker)
    @assert worker.status == "starting"

    start_listening(worker, worker.port_to_listen_on)

    info(logger, "      Start worker at: $(address(worker))")
    info(logger, "Waiting to connect to: $(worker.scheduler_address)")
    info(logger, string("-" ^ 49))

    register_worker(worker)
end

"""
    register_worker(worker::Worker)

Registers a `Worker` with the DASK scheduler.
"""
function register_worker(worker::Worker)
    @async begin

        send_msg(
            worker.comm,  # TODO: do we need to save this or is it a one time use thing?
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

        msg = consume(recv_msg(worker.comm))

        try
            @assert msg == "OK"
            worker.status = "running"
        catch
            error("An error ocurred on the dask-scheduler while registering: $msg")
        end
    end
end

function start_listening(worker::Worker, port::Integer)
    @async while isopen(worker.listener)
        worker.sock = accept(worker.listener)
        @async while isopen(worker.listener) && isopen(worker.sock)
            try
                msg = consume(recv_msg(worker.sock))
                address, port = getsockname(worker.sock)
                address = build_URI(address, port)
                debug(logger, "Message = from $address: $msg")
                if isa(msg, Array) && length(msg) == 1
                    msg = msg[1]
                end
                @sync handle_incoming_msg(worker, msg)
            catch exception
                # EOFErrors are expected when connections are closed unexpectadly
                isa(exception, EOFError) || rethrow(exception)
            end
        end
    end
end

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
        Base.close(worker.sock)
        Base.close(worker.comm)
        Base.close(worker.listener)
        # self._closed.set()
    end
end

function handle_incoming_msg(worker::Worker, msg::Dict)
    op = pop!(msg, "op", nothing)
    reply = pop!(msg, "reply", nothing)
    terminate = pop!(msg, "close", nothing)  # Figure out what to do with close
    msg = Dict((Symbol(k) => v) for (k, v) in msg)

    try
        handler = worker.handlers[op]
        result = handler(worker, ;msg...)

    catch exception
        error("No handler found for $op: $exception")
    end
end

##############################       HANDLER FUNCTIONS        ##############################

function compute_stream(worker::Worker)
    @async while isopen(worker.sock)
        warn(logger, "im inside compute_stream waiting")
        msgs = []

        try
            msgs = consume(recv_msg(worker.sock))
        catch exception
            isa(exception, EOFError) || rethrow(exception)
        end

        for msg in msgs
            info(logger, "msgs: $msg")
            address, port = getsockname(worker.sock)
            address = build_URI(address, port)
            debug(logger, "Message from $address: $msg")

            op = pop!(msg, "op", nothing)
            haskey(msg, "key") && validate_key(msg["key"])

            # Rename function to func in msg so that julia can deal with it
            haskey(msg, "function") && push!(msg, ("func" => pop!(msg, "function")))
            msg = Dict(parse(k) => v for (k,v) in msg)

            if op == "close"
                closed = true  # TODO: what is this used for
                close(worker)
                break
            elseif op == "compute-task"
                debug(logger, "$msg")
                add_task(worker, ;msg...)
            elseif op == "release-task"
                push!(worker.log, (msg[:key], "release-task"))  # FIXME colour of key is wrong
                release_key(worker, ;msg...)
            elseif op == "delete-data"
                delete_data(worker, ;msg...)
            else
                warn(logger, "Unknown operation $op, $msg")
            end
        end

        notice(logger, "about to call ensure_computing")
        ensure_computing(worker)
        notice(logger, "done while in compute_stream")
    end
    info(logger, "Close compute_stream")
end

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

function gather(worker::Worker)
    warn(logger, "Not implemented gather yet")
end

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
            @async report_to_scheduler(
                worker, "remove-keys", address=address(worker), keys=keys
            )
        end
    end
end

function terminate(worker::Worker, msg::Dict)
    warn(logger, "Not implemented terminate yet")
end

function get_keys(worker::Worker, msg::Dict)
    return keys(worker.data)  # TODO: return array instead of iterator maybe?
end


############################## COMPUTE-STREAM HELPER FUNCTIONS #############################

function add_task(
    worker::Worker; key::String="", duration=nothing, priority=nothing, func=nothing,
    who_has=nothing, nbytes=nothing, args=nothing, kwargs=nothing, task=nothing,
    resource_restrictions=nothing
)
    notice(logger, "In add_task")

    # TODO: figure out what should be done with priority counter
    if key == ""
        throw(ArgumentError("Key cannot be empty"))
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
        worker.priorities[key] = priority
        worker.durations[key] = duration
        return
    end

    notice(logger, "pushed new key onto log")
    push!(worker.log, (key, "new"))
    try
        start = time()
        worker.tasks[key] = deserialize_task(func, args, kwargs, task)
        stop = time()

        notice(logger, "done deserializing")

        if stop - start > 0.010
            if !haskey(worker.startstops, key)  # Remove if this doesnt need to be an []
                worker.startstops[key] = []
            end
            push!(worker.startstops[key], ("deserialize", start, stop))
        end
    catch exception
        error_msg = Dict(
            "exception" => "$(typeof(exception)))",
            "traceback" => sprint(showerror, exception),
            "key" => to_key(key),
            "op" => "task-erred",
        )
        warn(logger, "Could not deserialize task with key: \"$key\": $(error_msg["traceback"])")
        send_to_scheduler(worker, error_msg)
        push!(worker.log, (key, "deserialize-error"))
        return
    end

    worker.priorities[key] = priority
    worker.durations[key] = duration
    if resource_restrictions != nothing
        worker.resource_restrictions[key] = resource_restrictions
    end
    worker.task_state[key] = "waiting"

    if nbytes != nothing
        worker.nbytes[key] = nbytes
    end

    who_has = who_has != nothing ? who_has : Dict()
    worker.dependencies[key] = Set(who_has)
    worker.waiting_for_data[key] = Set()

    for dep in who_has
        if !haskey(worker.dependents, dep)
            worker.dependents[dep] = Set()
        end
        worker.dependents[dep].add(key)

        if !haskey(worker.dep_state, dep)
            if worker.task_state[dep] == "memory"  # TODO: maybe use get instead? or is the task_state guaranteed to have dep?
                worker.dep_state[dep] = "memory"
            else
                worker.dep_state[dep] = "waiting"
            end
        end

        if worker.dep_state[dep] != "memory"
            push!(worker.waiting_for_data[key], dep)
        end
    end

    for (dep, workers) in who_has  # FIXME colours are off
        @assert workers != nothing
        if !haskey(worker.who_has, dep)
            worker.who_has[dep] = set(workers)
        end
        push!(worker.who_has[dep], workers)

        for worker in workers
            push!(worker.has_what[worker], dep)
            if worker.dep_state[dep] != "memory"
                push!(worker.pending_data_per_worker[worker], dep)
            end
        end
    end

    if !isempty(worker.waiting_for_data[key])
        push!(worker.data_needed, key)
    else
        transition(worker, key, "ready")
    end

    if worker.validate && !isempty(who_has)
        @assert all(dep in worker.dep_state for dep in who_has)
        @assert all(dep in self.nbytes for dep in who_has)
        for dep in who_has
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

    # if key in worker.threads
    #     del worker.threads[key]
    # TODO: is priorities and durations only needed for multi threading?
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
        send_to_scheduler(
            worker,
            Dict("op" => "release", "key" => to_key(key), "cause" => cause)
        )
    end

    notice(logger, "done release_key with key released: $key")
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

        if haskey(worker.in_flight_tasks, dep)
            delete!(worker.in_flight_tasks, dep)
        end

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
    # notice(logger, "in ensure_computing")
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
        key = Collections.dequeue!(worker.ready)
        if worker.task_state[key] in READY
            transition(worker, key, "executing")
        end
    end
    # notice(logger, "done ensure_computing")
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

        # start = time()
        # TODO: pack and unpack data stuff
        # args2 = pack_data(args, worker.data, key_types=str)
        # kwargs2 = pack_data(kwargs, worker.data, key_types=str)

        # stop = time()
        # if stop - start > 0.005
            # push!(worker.startstops[key], ("disk-read", start, stop))
            # if worker.digests is not None
            #     worker.digests["disk-load-duration"].add(stop - start)
        # end

        debug(logger, "Execute key $key worker $(address(worker))")

        # calculate result from remoteref (<-TODO), find a way to notify clients that the computation is done
        result = apply_function(func, args, kwargs)
        notice(logger, "the result was $result")

        if worker.task_state[key] ∉ ("executing", "long-running")
            return
        end

        result["key"] = key
        value = pop!(result, "result", nothing)

        !haskey(worker.startstops, key) && (worker.startstops[key] = [])
        push!(worker.startstops[key], ("compute", result["start"], result["stop"]))

        if result["op"] == "task-finished"
            worker.nbytes[key] = result["nbytes"]
            worker.types[key] = result["type"]
            transition(worker, key, "memory", value=value)
        else
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
        warn(logger, "done execute")
        # end
    end
end

##############################      TRANSITION FUNCTIONS      ##############################

function transition(worker::Worker, key::String, to_state::String; kwargs...)
    notice(logger, "In transition: transitioning key $key to $to_state")
    from_state = worker.task_state[key]

    if from_state == to_state
        warn(logger, "Called transition method with same start and end state")
        return
    end

    transition_func = worker.transitions[from_state, to_state]
    info(logger, "kwargs: $kwargs")
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
        # TODO: why is priorities sent as a list from scheduler? here im just grabbing the first for now
        Collections.enqueue!(worker.ready, key, parse(worker.priorities[key][1]))
        return "ready"
    end
end

function transition_waiting_memory(worker::Worker, key::String, value::Any=nothing)
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
        @assert all(dep in worker.data for dep in worker.dependencies[key])
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
    transition_ready_executing(worker, key)
    for (resource, quantity) in worker.resource_restrictions[key]
        worker.available_resources[resource] -= quantity
    end

    if worker.validate
        @assert all(v >= 0 for v in values(worker.available_resources))
    end
    return "executing"
end

function transition_executing_done(worker::Worker, key::String; value::Any=nothing)
    # TODO: is this where the sentinel nothing value was used? if yes we need it
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

    if value != nothing
        worker.task_state[key] = "memory"
        put_key_in_memory(worker, key, value)
        if haskey(worker.dep_state, key)
            transition_dep(worker, key, "memory")
        end
    end

    if isopen(worker.sock)
        send_task_state_to_scheduler(worker, key)
    else
        error("Connection closed in transition_executing_done")  # TODO: throw better error?
    end

    notice(logger, "done transition_executing_done")

    return "done"
end

function transition_executing_long_running(worker::Worker, key::String)
    if worker.validate
        @assert key in worker.executing
    end

    delete!(worker.executing, key)
    push!(worker.long_running, key)
    # TODO: make sure worker.sock and worker.comm are being used correctly
    send_msg(worker.comm, Dict("op" => "long-running", "key" => key))

    ensure_computing(worker)
    return "long-running"
end

# TODO: rename/move section
##############################      TRANSITION HELPERS?       ##############################

function put_key_in_memory(worker::Worker, key::String, value)
    if !haskey(worker.data, key)

        start = time()
        worker.data[key] = value
        stop = time()
        if stop - start > 0.020  # TODO: is start stops even something we care about?
            push!(worker.startstops[key], ("disk-write", start, stop))
        end

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

        if haskey(worker.task_state, key) && worker.task_state[key] != "memory"
            transition(worker, key, "memory")
        end

        push!(worker.log, (key, "put-in-memory"))
        notice(logger, "done put-key-in-memory")
    end
end


##############################      VALIDATION FUNCTIONS      ##############################

function validate_key_memory(worker::Worker, key::String)
    @assert key in worker.data
    @assert key in worker.nbytes
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
    @assert key in pluck(1, worker.ready)  # TODO: what does this pluck do, implement
    @assert !haskey(worker.data, key)
    @assert !haskey(worker.executing, key)
    @assert !haskey(worker.waiting_for_data, key)
    @assert all(dep in worker.data for dep in worker.dependencies[key])
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
    @assert dep in worker.nbytes
    @assert worker.dependents[dep]
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
    @assert dep in worker.data
    @assert dep in worker.nbytes
    @assert dep in worker.types
    if dep in worker.task_state
       @assert worker.task_state[dep] == "memory"
    end
end

function validate_dep(worker::Worker, key::String)
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

send_to_scheduler(worker::Worker, msg::Dict) = send_msg(worker.sock, msg)

function send_task_state_to_scheduler(worker::Worker, key::String)
    notice(logger, "in send_task_state_to_scheduler")
    if haskey(worker.data, key)
        nbytes = get(worker.nbytes, key, sizeof(worker.data[key]))
        oftype = get(worker.types, key, typeof(worker.data[key]))

        msg = Dict{String, Any}(
            "op" => "task-finished",
            "status" => "OK",
            "key" => to_key(key),
            "nbytes" => nbytes,
            "type" => to_serialize(oftype),
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

    @async send_msg(worker.sock, msg)

    notice(logger, "done send_task_state_to_scheduler")
end


function report_to_scheduler(worker::Worker, action::String; address=nothing, keys=nothing)
    # TODO: implement the rest of this
    if action == "remove_keys"
        msg = Dict(
            "op" => "task-finished",
            "status" => "OK",
            "key" => to_key(key),
            # "nbytes": nbytes,
            # "thread": threads.get(key),
            # "type": typ
        )
    else
        error("action: $action not impleemented yet")
    end
    send_msg(worker.comm, msg)
end

##############################         OTHER FUNCTIONS        ##############################

""" Deserialize task inputs and regularize to func, args, kwargs """
function deserialize_task(func, args, kwargs, task)
    notice(logger, "in deserialize_task")
    warn(logger, "the items were: $func, $args, $kwargs, $task")
    if func != nothing
        func = to_deserialize(func)
    end
    if args != nothing
        args = to_deserialize(args)
    end
    if kwargs != nothing
        kwargs = to_deserialize(kwargs)
    end

    if task != nothing
        @assert is(func, nothing) && is(args, nothing) && is(kwargs, nothing)
        func = execute_task
        args = (task,)
    end

    # args = [parse(arg) for arg in args]  # Probably don't need to parse kwargs anymore either
    # TODO: cleanup
    if kwargs != nothing
        kwargs = Dict{String, Any}(k => parse(v) for (k,v) in kwargs)
    else
        kwargs = Dict{String, Any}()
    end
    debug(logger, "$((func, args, kwargs))")

    return (func, args, kwargs)
end

# TODO: update documentation
""" Evaluate a nested task

>>> inc = lambda x: x + 1
>>> execute_task((inc, 1))
2
>>> execute_task((sum, [1, 2, (inc, 3)]))
7
"""
function execute_task(task)
    notice(logger, "worker is in execute_task")
    if is_task(task)
        func, args = task[1], task[2:end]
        return func(map(execute_task, args)...)
    elseif isa(task, Array)
        return [map(execute_task, task)]
    else
        return task
    end
end

# TODO: update documentation
""" Run a function, collect information

Returns
-------
result_msg: dictionary with status, result/error, timings, etc..
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
        result_msg["type"] = !is(result, nothing ) ? typeof(result) : nothing
    catch exception
        result_msg = Dict{String, Any}(
            "exception" => "$(typeof(exception))",
            "traceback" => sprint(showerror, exception),  # TODO: is traceback actually needed?
            "op" => "task-erred"
        )
    end
    stop = time()
    result_msg["start"] = start
    result_msg["stop"] = stop
    return result_msg
end
