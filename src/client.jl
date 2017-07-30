const SHUTDOWN = ("closed", "closing")


"""
    Client

Client that can be interacted with to submit computations to the scheduler and gather
results. Should only be used directly for advanced workflows. See [`DaskExecutor`](@ref)
instead for normal usage.

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
    nodes::Dict{String, Void}
    id::String
    status::String
    scheduler_address::Address
    scheduler::Rpc
    connecting_to_scheduler::Bool
    scheduler_comm::Nullable{BatchedSend}
    pending_msg_buffer::Array{Dict, 1}
end

"""
    Client(scheduler_address::String) -> Client

Construct a `Client` which can then be used to submit computations or gather results from
the dask-scheduler process.
"""
function Client(scheduler_address::String)
    scheduler_address = Address(scheduler_address)

    client = Client(
        Dict{String, DispatchNode}(),
        "$(Base.Random.uuid1())",
        "connecting",
        scheduler_address,
        Rpc(scheduler_address),
        false,
        nothing,
        Dict[],
    )
    ensure_connected(client)
    return client
end

"""
    ensure_connected(client::Client)

Ensure the `client` is connected to the dask-scheduler and if not register it as the
default Dask scheduler client. For internal use.
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

            get(response, "op", nothing) == "stream-start" || error("Error: $response")


            client.scheduler_comm = BatchedSend(comm, interval=0.01)
            client.connecting_to_scheduler = false

            client.status = "running"

            while !isempty(client.pending_msg_buffer)
                send_msg(get(client.scheduler_comm), pop!(client.pending_msg_buffer))
            end
        end
    end
end

"""
    send_to_scheduler(client::Client, msg::Dict)

Send `msg` to the dask-scheduler that the client is connected to. For internal use.
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
    client.status ∉ SHUTDOWN || error("Client not running. Status: \"$(client.status)\"")

    if !haskey(client.nodes, get_key(node))
        tkey, task, unprocessed_deps, task_dependencies = serialize_node(client, node)

        tkeys = [tkey]
        tasks = Dict(tkey => task)
        tasks_deps = Dict(tkey => task_dependencies)

        tkeys, tasks, tasks_deps = serialize_deps(
            client, unprocessed_deps, tkeys, tasks, tasks_deps
        )

        restrictions = !isempty(workers) ? Dict(tkey => workers) : Dict()

        send_to_scheduler(
            client,
            Dict(
                "op" => "update-graph",
                "keys" => tkeys,
                "tasks" => tasks,
                "dependencies" => tasks_deps,
                "restrictions" => restrictions,
            )
        )
        for tkey in tkeys
            client.nodes[convert(String, tkey)] = nothing
        end
    end
end

"""
    cancel{T<:DispatchNode}(client::Client, nodes::Array{T, 1})

Cancel all `DispatchNode`s in `nodes`. This stops future tasks from being scheduled
if they have not yet run and deletes them if they have already run. After calling, this
result and all dependent results will no longer be accessible.
"""
function cancel{T<:DispatchNode}(client::Client, nodes::Array{T, 1})
    client.status ∉ SHUTDOWN || error("Client not running. Status: \"$(client.status)\"")

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
        push!(results, fetch(node))
    end
    return results
end

"""
    replicate{T<:DispatchNode}(client::Client; nodes::Array{T, 1}=DispatchNode[])

Copy data onto many workers. Helps to broadcast frequently accessed data and improve
resilience.
"""
function replicate{T<:DispatchNode}(client::Client; nodes::Array{T, 1}=DispatchNode[])
    client.status ∉ SHUTDOWN || error("Client not running. Status: \"$(client.status)\"")

    if isempty(nodes)
        keys_to_replicate = [to_key(key) for key in keys(client.nodes)]
    else
        keys_to_replicate = [to_key(get_key(node)) for node in nodes]
    end
    msg = Dict("op" => "replicate", "keys"=> keys_to_replicate)
    send_recv(client.scheduler, msg)
end

"""
    shutdown(client::Client)

Tell the dask-scheduler that this client is shutting down. Does NOT terminate the scheduler
itself nor the workers. This does not have to be called after a session but is useful to
delete all the information submitted by the client from the scheduler and workers (such as
between test runs). To reconnect to the scheduler after calling this function set up a new
client.
"""
function shutdown(client::Client)
    client.status ∉ SHUTDOWN || error("Client not running. Status: \"$(client.status)\"")
    client.status = "closing"

    # Tell scheduler that this client is shutting down
    send_msg(get(client.scheduler_comm), Dict("op" => "close-stream"))
    client.status = "closed"
end

##############################  DISPATCHNODE KEYS FUNCTIONS   ##############################

"""
    get_key{T<:DispatchNode}(node::T)

Calculate an identifying key for `node`. Keys are re-used for identical `nodes` to avoid
unnecessary computations.
"""
get_key{T<:DispatchNode}(node::T) = error("$T does not implement get_key")

function get_key(node::Op)
    return string(
        get_label(node), "-", hash((node.func, node.args, node.kwargs, node.result))
    )
end

function get_key(node::IndexNode)
    return string("Index", string(node.index), "-", hash((node.node, node.result)))
end

function get_key(node::CollectNode)
    return string(get_label(node), "-", hash((node.nodes, node.result)))
end

function get_key(node::DataNode)
    return string("Data", "-", hash((node.data)))
end

##############################     SERIALIZATION FUNCTIONS    ##############################

"""
    serialize_deps(client::Client, deps::Array, tkeys::Array, tasks::Dict, tasks_deps::Dict)

Serialize all dependencies in `deps` to send to the scheduler. For internal use.

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
    return tkeys, tasks, tasks_deps
end

"""
    serialize_node(client::Client, node::DispatchNode)

Serialize all dependencies in `deps` to send to the scheduler. For internal use.

# Returns
- `tkey::Array`: the key that will be sent to the scheduler in byte form
- `task::Dict`: serialized task that will be sent to the scheduler
- `unprocessed_deps::Array`: list of dependencies that haven't been serialized yet
- `task_dependencies::Array`: the keys of the dependencies for `task`
"""
function serialize_node(client::Client, node::DispatchNode)
    # Get task dependencies
    deps = collect(DispatchNode, dependencies(node))
    unprocessed_deps = filter(dep->!haskey(client.nodes, get_key(dep)), deps)

    task_dependencies = [to_key(get_key(dep)) for dep in deps]
    task = serialize_task(client, node, deps)

    return to_key(get_key(node)), task, deps, task_dependencies
end

"""
    serialize_task{T<:DispatchNode}(client::Client, node::T, deps::Array) -> Dict

Serialize `node` into its components. For internal use.
"""
function serialize_task{T<:DispatchNode}(client::Client, node::T, deps::Array)
    error("$T does not implement serialize_task")
end

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
        "func" => to_serialize(unpack_data((x)->x[node.index])),
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

function serialize_task(client::Client, node::DataNode, deps::Array)
    return Dict(
        "func" => to_serialize(unpack_data((x)->x)),
        "args" => to_serialize(unpack_data(node.data)),
    )
end








