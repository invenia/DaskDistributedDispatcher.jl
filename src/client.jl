global_client = []

const SHUTDOWN = ("closed", "closing")


"""
    Client

Client that the user can interact with to submit computations to the scheduler and gather
results.

# Fields
- `nodes::Dict{String, DispatchNode}`: maps keys to their dispatcher `DispatchNode`
- `id::String`: this client's identifier
- `status::String`: status of this client
- `scheduler_address::Address`: the dask-distributed scheduler ip address and port information
- ` scheduler::Rpc`: manager for discrete send/receive open connections to the scheduler
- `connecting_to_scheduler::Bool`: if client is currently trying to connect to the scheduler
- `scheduler_comm::Nullable{BatchedSend}`: batched stream for communication with scheduler
- `pending_msg_buffer::Array`: pending msgs to send on the batched stream
"""
type Client
    nodes::Dict{String, DispatchNode}
    id::String
    status::String
    scheduler_address::Address
    scheduler::Rpc
    connecting_to_scheduler::Bool
    scheduler_comm::Nullable{BatchedSend}
    pending_msg_buffer::Array
    throw_errors::Bool
end

"""
    Client(scheduler_address::String; throw_errors::Bool=true) -> Client

Construct a `Client` which can then be used to submit computations or gather results from
the dask-scheduler process. Set throw_errors to `false` if you want to instead receive
string representations of any errors thrown while running a user submitted task.
"""
function Client(scheduler_address::String; throw_errors::Bool=true)
    scheduler_address = Address(scheduler_address)
    client = Client(
        Dict{String, DispatchNode}(),
        "$(Base.Random.uuid1())",
        "connecting",
        scheduler_address,
        Rpc(scheduler_address),
        false,
        nothing,
        [],
        throw_errors,
    )
    ensure_connected(client)
    return client
end

"""
    ensure_connected(client::Client)

Ensure the `client` is connected to the dask-scheduler and if not register it as the
default Dask scheduler client.
"""
function ensure_connected(client::Client)
    @async begin
        if (
            (isnull(client.scheduler_comm) || !isopen(get(client.scheduler_comm).comm)) &&
            !client.connecting_to_scheduler
        )
            client.connecting_to_scheduler = true
            comm = connect(client.scheduler_address)
            response = send_recv(
                comm,
                Dict("op" => "register-client", "client" => client.id, "reply"=> false)
            )

            get(response, "op", "") == "stream-start" || error("Error: $response")


            client.scheduler_comm = BatchedSend(comm, interval=0.01)
            client.connecting_to_scheduler = false

            push!(global_client, client)
            client.status = "running"

            while !isempty(client.pending_msg_buffer)
                send_msg(get(client.scheduler_comm), pop!(client.pending_msg_buffer))
            end
        end
    end
end

"""
    send_to_scheduler(client::Client, msg::Dict)

Send `msg` to the dask-scheduler that the client is connected to.
"""
function send_to_scheduler(client::Client, msg::Dict)
    if client.status == "running"
        send_msg(get(client.scheduler_comm), msg)
    elseif client.status == "connecting"
        push!(client.pending_msg_buffer, msg)
    else
        error("Client not running. Status: \"$(client.status)\"")
    end
end

"""
    submit(client::Client, node::DispatchNode; workers::Array{Address,1}=Array{Address,1}())

Submit the `node` computation unit to the dask-scheduler for computation. Also submits all
`node`'s dependencies to the scheduler if they have not previously been submitted.
"""
function submit(client::Client, node::DispatchNode; workers::Array{Address,1}=Array{Address,1}())
    if client.status ∉ SHUTDOWN
        key = get_key(node)

        if !haskey(client.nodes, key)
            tkey, task, unprocessed_deps, task_dependencies = serialize_node(client, node)

            tkeys = [tkey]
            tasks = Dict(tkey => task)
            tasks_deps = Dict(tkey => task_dependencies)

            tkeys, tasks, tasks_deps = serialize_deps(
                client, unprocessed_deps, tkeys, tasks, tasks_deps
            )

            restrictions = !isempty(workers) ? Dict(tkey => workers) : Dict()

            msg = Dict(
                "op" => "update-graph",
                "keys" => tkeys,
                "tasks" => tasks,
                "dependencies" => tasks_deps,
                "restrictions" => restrictions,
            )
            send_to_scheduler(client, msg)
        end
    else
        error("Client not running. Status: \"$(client.status)\"")
    end
end

"""
    result(client::Client, node::DispatchNode) -> Any

Gather the result of the `DispatchNode` computation unit. Requires there to be at least one
worker available to the scheduler or hangs indefinetely.
"""
function result(client::Client, node::DispatchNode)
    if isready(node)
        return fetch(node)
    end

    key = get_key(node)

    # Resuse a previously computed value if possible or wait for result
    result = nothing
    if haskey(client.nodes, key)
        result = fetch(client.nodes[key])

        if client.nodes[key] != node
            put!(node.result, result)
        end
    else
        error("The client does not have the requested node: \"$node\"")
    end

    if isa(result, Pair) && result.first == "error"
        if client.throw_errors
            rethrow(result.second)
        end
    end
    return result
end

"""
    cancel{T<:DispatchNode}(client::Client, nodes::Array{T, 1})

Cancel all `DispatchNode`s in `nodes`. This stops future tasks from being scheduled
if they have not yet run and deletes them if they have already run. After calling, this
result and all dependent results will no longer be accessible.
"""
function cancel{T<:DispatchNode}(client::Client, nodes::Array{T, 1})
    keys = [get_key(node) for node in nodes]
    tkeys = [to_key(key) for key in keys]
    send_recv(
        client.scheduler,
        Dict("op" => "cancel", "keys" => tkeys, "client" => client.id)
    )

    for key in keys
        delete!(client.nodes, key)
    end
end

"""
    gather{T<:DispatchNode}(client::Client, nodes::Array{T, 1})

Gather the results of all `nodes`. Requires there to be at least one worker
available to the scheduler or hangs indefinetely waiting for the results.
"""
function gather{T<:DispatchNode}(client::Client, nodes::Array{T, 1})
    if client.status ∉ SHUTDOWN
        send_to_scheduler(
            client,
            Dict(
                "op" => "client-desires-keys",
                "keys" => [to_key(get_key(node)) for node in nodes],
                "client" => client.id
            )
        )
    end
    results = []
    for node in nodes
        push!(results, result(client, node))
    end
    return results
end

"""
    replicate{T<:DispatchNode}(client::Client; nodes::Array{T, 1}=DispatchNode[])

Copy data onto many workers. Helps to broadcast frequently accessed data and improve
resilience.
"""
function replicate{T<:DispatchNode}(client::Client; nodes::Array{T, 1}=DispatchNode[])
    if isempty(nodes)
        nodes = collect(values(client.nodes))
    end
    keys_to_replicate = [to_key(get_key(node)) for node in nodes]
    msg = Dict("op" => "replicate", "keys"=> keys_to_replicate)
    send_recv(client.scheduler, msg)
end

"""
    shutdown(client::Client)

Tell the dask-scheduler that this client is shutting down. Does NOT terminate the scheduler
itself nor the workers. This does not have to be called after a session
but is useful when you want to delete all the information submitted by the client from
the scheduler and workers (such as between test runs). If you want to reconnect to the
scheduler after calling this function you will have to set up a new client.
"""
function shutdown(client::Client)
    if client.status ∉ SHUTDOWN
        client.status = "closing"

        # Tell scheduler that this client is shutting down
        send_msg(get(client.scheduler_comm), Dict("op" => "close-stream"))

        # Remove default client
        if !isempty(global_client) && global_client[1] == client
            pop!(global_client)
        end
        client.status = "closed"
    else
        error("Client not running. Status: \"$(client.status)\"")
    end
end

"""
    default_client()

Return the default global client if a client has been registered with the dask-scheduler.
"""
function default_client()
    if !isempty(global_client)
        return global_client[1]
    else
        error(
            "No clients found\n" *
            "Start a client and point it to the scheduler address\n" *
            "    client = Client(\"ip-addr-of-scheduler:8786\")\n"
        )
    end
end

##############################  DISPATCHNODE KEYS FUNCTIONS   ##############################

"""
    get_key(node::DispatchNode)

Calculate an identifying key for `node`. Keys are re-used for identical `nodes` to avoid
unnecessary computations.
"""
function get_key(node::Op)
    return string(get_label(node), "-", hash((node.func, node.args, node.kwargs)))
end

function get_key(node::IndexNode)
    return string("Index", string(node.index), "-", hash(node.node))
end

function get_key(node::CollectNode)
    return string(get_label(node), "-", hash((node.nodes)))
end

##############################     SERIALIZATION FUNCTIONS    ##############################

"""
    serialize_deps(client::Client, deps::Array, tkeys::Array, tasks::Dict, tasks_deps::Dict)

Serialize all dependencies in `deps` to send to the scheduler.

# Returns
- `tkeys::Array`: the keys that will be sent to the scheduler in byte form
- `tasks::Dict`: the serialized tasks that will be sent to the scheduler
- `tasks_deps::Dict`: the keys of the dependencies for each task
"""
function serialize_deps(
    client::Client,
    deps::Array,
    tkeys::Array,
    tasks::Dict,
    tasks_deps::Dict
)
    for dep in deps
        key = get_key(dep)

        if !haskey(client.nodes, key)
            tkey, task, unprocessed_deps, task_dependencies = serialize_node(client, dep)

            push!(tkeys, tkey)
            tasks[tkey] = task
            tasks_deps[tkey] = task_dependencies

            if !isempty(unprocessed_deps)
                tkeys, tasks, tasks_deps = serialize_deps(
                    client, unprocessed_deps, tkeys, tasks, tasks_deps
                )
            end
        end
    end
    return tkeys, tasks, tasks_deps
end

"""
    serialize_node(client::Client, node::DispatchNode)

Serialize all dependencies in `deps` to send to the scheduler.

# Returns
- `tkey::Array`: the key that will be sent to the scheduler in byte form
- `task::Dict`: serialized task that will be sent to the scheduler
- `unprocessed_deps::Array`: list of dependencies that haven't been serialized yet
- `task_dependencies::Array`: the keys of the dependencies for `task`
"""
function serialize_node(client::Client, node::DispatchNode)
    key = get_key(node)

    if !haskey(client.nodes, key)
        tkey = to_key(key)

        # Get task dependencies
        deps = collect(dependencies(node))
        task_dependencies = [to_key(get_key(dep)) for dep in deps]

        task = serialize_task(client, node, deps)

        client.nodes[key] = node
        unprocessed_deps = filter!(dep->!haskey(client.nodes, dep), deps)

        return tkey, task, unprocessed_deps, task_dependencies
    end
end

"""
    serialize_task(client::Client, node::DispatchNode, deps::Array) -> Dict

Serialize `node` into its components.
"""
function serialize_task(client::Client, node::Op, deps::Array)
    return Dict(
        "func" => to_serialize(unpack_data(node.func)),
        "args" => to_serialize(unpack_data(node.args)),
        "kwargs" => to_serialize(unpack_data(node.kwargs)),
        "future" => to_serialize(node.result),
    )
end

function serialize_task(client::Client, node::IndexNode, deps::Array)
    return Dict(
        "func" => to_serialize(unpack_data((x)->x)),
        "args" => to_serialize(unpack_data(deps)),
        "future" => to_serialize(node.result),
    )
end

function serialize_task(client::Client, node::CollectNode, deps::Array)
    return Dict(
        "func" => to_serialize(unpack_data(Array)),
        "args" => to_serialize((unpack_data(deps),)),
        "future" => to_serialize(node.result),
    )
end








