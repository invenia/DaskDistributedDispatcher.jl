const SHUTDOWN = (:closed, :closing)


"""
    Client

Client that can be interacted with to submit computations to the scheduler and gather
results. Should only be used directly for advanced workflows. See [`DaskExecutor`](@ref)
instead for normal usage.

# Fields
- `keys::Set{String}`: previously submitted keys
- `id::String`: this client's identifier
- `status::String`: status of this client
- `scheduler_address::Address`: the dask-distributed scheduler ip address and port info
- `scheduler::Rpc`: manager for discrete send/receive open connections to the scheduler
- `scheduler_comm::Nullable{BatchedSend}`: batched stream for communication with scheduler
- `pending_msg_buffer::Vector{Dict{String, Any}}`: pending msgs to send to the scheduler
"""
type Client
    keys::Set{String}
    id::String
    status::Symbol
    scheduler_address::Address
    scheduler::Rpc
    scheduler_comm::Nullable{BatchedSend}
    pending_msg_buffer::Vector{Dict{String, Any}}
end

"""
    Client(scheduler_address::String) -> Client

Construct a `Client` which can then be used to submit computations or gather results from
the dask-scheduler process.


## Usage

```julia
using DaskDistributedDispatcher
using Dispatcher

addprocs(3)
@everywhere using DaskDistributedDispatcher

for i in 1:3
    @spawn Worker("127.0.0.1:8786")
end

client = Client("127.0.0.1:8786")

op = Op(Int, 2.0)
submit(client, op)
result = fetch(op)
```

Previously submitted `Ops` can be cancelled by calling:

```julia
cancel(client, [op])

# Or if using the `DaskExecutor`
cancel(executor.client, [op])
```

If needed, which worker(s) to run the computations on can be explicitly specified by
returning the worker's address when starting a new worker:

```julia
using DaskDistributedDispatcher
client = Client("127.0.0.1:8786")

pnums = addprocs(1)
@everywhere using DaskDistributedDispatcher

worker_address = @fetchfrom pnums[1] begin
    worker = Worker("127.0.0.1:8786")
    return worker.address
end

op = Op(Int, 1.0)
submit(client, op, workers=[worker_address])
result = result(client, op)
```
"""
function Client(scheduler_address::String="127.0.0.1:8786")
    scheduler_address = Address(scheduler_address)

    client = Client(
        Set{String}(),
        "$(Base.Random.uuid1())",
        :starting,
        scheduler_address,
        Rpc(scheduler_address),
        Nullable(),
        Dict{String, Any}[],
    )
    ensure_connected(client)
    return client
end

"""
    ensure_connected(client::Client)

Ensure the `client` is connected to the dask-scheduler. For internal use.
"""
function ensure_connected(client::Client)
    @schedule begin
        if (
            (isnull(client.scheduler_comm) || !isopen(get(client.scheduler_comm).comm)) &&
            client.status !== :connecting
        )
            client.status = :connecting

            comm = connect(client.scheduler_address)
            response = send_recv(
                comm,
                Dict{String, Any}(
                    "op" => "register-client",
                    "client" => client.id,
                    "reply"=> false,
                )
            )

            get(response, "op", nothing) == "stream-start" || error("Error: $response")

            client.scheduler_comm = BatchedSend(comm, interval=0.01)
            client.status = :running

            while !isempty(client.pending_msg_buffer)
                send_to_scheduler(client, pop!(client.pending_msg_buffer))
            end
        end
    end
end

"""
    send_to_scheduler(client::Client, msg::Dict{String, Any})

Send `msg` to the dask-scheduler that the client is connected to. For internal use.
"""
function send_to_scheduler(client::Client, msg::Dict{String, Any})
    if client.status === :running
        send_msg(get(client.scheduler_comm), msg)
    elseif client.status === :connecting || client.status === :starting
        push!(client.pending_msg_buffer, msg)
    else
        error("Client not running. Status: \"$(client.status)\"")
    end
end

"""
    submit(client::Client, node::DispatchNode; workers::Vector{Address}=Address[])

Submit the `node` computation unit to the dask-scheduler for computation. Also submits all
`node`'s dependencies to the scheduler if they have not previously been submitted.
"""
function submit(client::Client, node::DispatchNode; workers::Vector{Address}=Address[])
    client.status ∉ SHUTDOWN || error("Client not running. Status: \"$(client.status)\"")

    key = get_key(node)

    if key ∉ client.keys
        deps, task, task_dependencies = serialize_node(client, node)

        keys = String[key]
        tasks = Dict{String, Dict{String, Vector{UInt8}}}(key => task)
        tasks_deps = Dict{String, Vector{String}}(key => task_dependencies)

        keys, tasks, tasks_deps = serialize_deps(client, deps, keys, tasks, tasks_deps)

        restrictions = Dict{String, Vector{Address}}(key => workers)

        send_to_scheduler(
            client,
            Dict{String, Any}(
                "op" => "update-graph",
                "keys" => keys,
                "tasks" => tasks,
                "dependencies" => tasks_deps,
                "restrictions" => restrictions,
            )
        )

        push!(client.keys, keys...)
    end
end

"""
    cancel{T<:DispatchNode}(client::Client, nodes::Vector{T})

Cancel all `DispatchNode`s in `nodes`. This stops future tasks from being scheduled
if they have not yet run and deletes them if they have already run. After calling, this
result and all dependent results will no longer be accessible.
"""
function cancel{T<:DispatchNode}(client::Client, nodes::Vector{T})
    client.status ∉ SHUTDOWN || error("Client not running. Status: \"$(client.status)\"")

    keys = map(get_key, nodes)
    send_recv(
        client.scheduler,
        Dict{String, Any}("op" => "cancel", "keys" => keys, "client" => client.id)
    )

    for key in keys
        delete!(client.keys, key)
    end
end

"""
    gather{T<:DispatchNode}(client::Client, nodes::Vector{T}) -> Vector

Gather the results of all `nodes`. Requires there to be at least one worker
available to the scheduler or hangs indefinetely waiting for the results.
"""
function gather{T<:DispatchNode}(client::Client, nodes::Vector{T})
    if client.status ∉ SHUTDOWN
        send_to_scheduler(
            client,
            Dict{String, Any}(
                "op" => "client-desires-keys",
                "keys" => map(get_key, nodes),
                "client" => client.id,
            )
        )
    end
    return map(fetch, nodes)
end

"""
    replicate{T<:DispatchNode}(client::Client; nodes::Vector{T}=DispatchNode[])

Copy data onto many workers. Helps to broadcast frequently accessed data and improve
resilience. By default replicates all nodes that have been submitted by this client unless
they have been cancelled.
"""
function replicate{T<:DispatchNode}(client::Client; nodes::Vector{T}=DispatchNode[])
    client.status ∉ SHUTDOWN || error("Client not running. Status: \"$(client.status)\"")

    keys_to_replicate = isempty(nodes) ? collect(String, client.keys) : map(get_key, nodes)

    send_recv(
        client.scheduler,
        Dict{String, Union{String, Vector{String}}}(
            "op" => "replicate",
            "keys" => keys_to_replicate,
        )
    )
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
    client.status = :closing

    # Tell scheduler that this client is shutting down
    send_msg(get(client.scheduler_comm), Dict("op" => "close-stream"))
    client.status = :closed
end

##############################  DISPATCHNODE KEYS FUNCTIONS   ##############################

"""
    get_key{T<:DispatchNode}(node::T)

Calculate an identifying key for `node`. Keys are re-used for identical `nodes` to avoid
unnecessary computations.
"""
get_key{T<:DispatchNode}(node::T) = error("$T does not implement get_key")

get_key(node::Op) = string(get_label(node), "-", hash(node))

get_key(node::IndexNode) = string("Index", node.index, "-", hash(node))

get_key(node::CollectNode) = string("Collect-", hash(node))

get_key(node::DataNode) = string("Data-", hash(node.data))

##############################     SERIALIZATION FUNCTIONS    ##############################

"""
    serialize_deps{T<:DispatchNode}(args...) -> Tuple

Serialize dependencies to send to the scheduler.

# Arguments
- `client::Client`
- `deps::Vector{T}`: the node dependencies to be serialized
- `keys::Vector{String}`: list of all keys that have already been serialized
- `tasks::Dict{String, Dict{String, Vector{UInt8}}}`: serialized tasks
- `tasks_deps::Dict{String, Vector{String}}`: dependencies for each task

# Returns
- `Tuple{Vector{String}, Dict{String, Dict{String, Vector{UInt8}}}, Dict{String, Vector{String}}}`:
    keys, serialized tasks, and task dependencies that will be sent to the scheduler
"""
function serialize_deps{T<:DispatchNode}(
    client::Client,
    deps::Vector{T},
    keys::Vector{String},
    tasks::Dict{String, Dict{String, Vector{UInt8}}},
    tasks_deps::Dict{String, Vector{String}},
)
    for dep in deps
        key = get_key(dep)

        key in keys && continue
        deps, task, task_dependencies = serialize_node(client, dep)

        push!(keys, key)
        tasks[key] = task
        tasks_deps[key] = task_dependencies

        if !isempty(deps)
            keys, tasks, tasks_deps = serialize_deps(client, deps, keys, tasks, tasks_deps)
        end
    end
    return keys, tasks, tasks_deps
end

"""
    serialize_node(client::Client, node::DispatchNode) -> Tuple

Serialize `node` into it's task and dependencies. For internal use.

# Returns
- `Tuple{Vector{DispatchNode}, Dict{String, Vector{UInt8}}, Vector{String}}`: tuple of the
    task dependencies nodes that are yet to be serialized, the serialized task, and the
    keys of the serialized task's dependencies that will be sent to the scheduler
"""
function serialize_node(client::Client, node::DispatchNode)
    deps = collect(DispatchNode, dependencies(node))
    task = serialize_task(client, node, deps)
    task_dependencies = map(get_key, deps)

    return deps, task, task_dependencies
end

"""
    serialize_task{T<:DispatchNode}(client::Client, node::T, deps::Vector{T}) -> Dict

Serialize `node` into its components. For internal use.
"""
function serialize_task{T<:DispatchNode}(client::Client, node::T, deps::Vector{T})
    error("$T does not implement serialize_task")
end

function serialize_task{T<:DispatchNode}(client::Client, node::Op, deps::Vector{T})
    return Dict{String, Vector{UInt8}}(
        "func" => to_serialize(unpack_data(node.func)),
        "args" => to_serialize(unpack_data(node.args)),
        "kwargs" => to_serialize(unpack_data(node.kwargs)),
        "future" => to_serialize(node.result),
    )
end

function serialize_task{T<:DispatchNode}(client::Client, node::IndexNode, deps::Vector{T})
    return Dict{String, Vector{UInt8}}(
        "func" => to_serialize((x)->x[node.index]),
        "args" => to_serialize((unpack_data(deps)...)),
        "future" => to_serialize(node.result),
    )
end

function serialize_task{T<:DispatchNode}(client::Client, node::CollectNode, deps::Vector{T})
    return Dict{String, Vector{UInt8}}(
        "func" => to_serialize((x)->x),
        "args" => to_serialize((unpack_data(deps),)),
        "future" => to_serialize(node.result),
    )
end

function serialize_task{T<:DispatchNode}(client::Client, node::DataNode, deps::Vector{T})
    return Dict{String, Vector{UInt8}}(
        "func" => to_serialize((x)->x[1]),
        "args" => to_serialize((unpack_data(node.data),)),
    )
end








