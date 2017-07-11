global_client = []

const SHUTDOWN = ("closed", "closing")


"""
    Client

A `Client` represents a client that the user can interact with to submit computations to
the scheduler and gather results.

# Fields
- `ops::Dict{String, Dispatcher.Op}`: maps keys to their dispatcher `ops`
- `id::String`: this client's identifier
- `status::String`: status of this client
- `scheduler_address::Address`: the dask-distributed scheduler ip address and port information
- ` scheduler::Rpc`: manager for discrete send/receive open connections to the scheduler
- `connecting_to_scheduler::Bool`: if client is currently trying to connect to the scheduler
- `scheduler_comm::Nullable{BatchedSend}`: batched stream for communication with scheduler
- `pending_msg_buffer::Array`: pending msgs to send on the batched stream
"""
type Client
    ops::Dict{String, Dispatcher.Op}
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
        Dict{String, Dispatcher.Op}(),
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
    submit(client::Client, op::Dispatcher.Op; workers::Array=[])

Submit the `Op` computation unit to the dask-scheduler for computation.
"""
function submit(client::Client, op::Dispatcher.Op; workers::Array=[])
    if client.status ∉ SHUTDOWN
        key = get_key(op)

        if !haskey(client.ops, key)
            tkey = to_key(key)

            # Get task dependencies
            dependencies = Dispatcher.dependencies(op)
            keys_needed = filter(k -> (k != key), [get_key(dep) for dep in dependencies])
            task_dependencies = Dict(tkey => collect(keys_needed))

            task = Dict(
                tkey => Dict(
                    "func" => to_serialize(unpack_data(op.func)),
                    "args" => to_serialize(unpack_data(op.args)),
                    "kwargs" => to_serialize(unpack_data(op.kwargs)),
                    "future" => to_serialize(op.result),
                ),
            )

            if !isempty(workers)
                restrictions = Dict(tkey => workers)
            else
                restrictions = Dict()
            end

            scheduler_op = Dict(
                "op" => "update-graph",
                "keys" => [tkey],
                "tasks" => task,
                "dependencies" => task_dependencies,
                "restrictions" => restrictions,
            )
            send_to_scheduler(client, scheduler_op)

            client.ops[key] = op
        end
    else
        error("Client not running. Status: \"$(client.status)\"")
    end
end

"""
    result(client::Client, op::Dispatcher.Op) -> Any

Gather the result of the `Op` computation unit. Requires there to be at least one worker
available to the scheduler or hangs indefinetely.
"""
function result(client::Client, op::Dispatcher.Op)
    if isready(op)
        return fetch(op)
    end

    # Wait for result
    key = get_key(op)
    result = nothing

    # Resuse a previously computed value if possible
    if haskey(client.ops, key)
        result = fetch(client.ops[key])
        if client.ops[key] != op
            put!(op.result, result)
        end
    else
        error("The client does not have the requested op: \"$op\"")
    end

    if isa(result, Pair) && result.first == "error"
        if client.throw_errors
            rethrow(result.second)
        end
    end
    return result
end

"""
    cancel(client::Client, ops::Array{Dispatcher.Op})

Cancel all `Op`s in `ops`. This stops future tasks from being scheduled
if they have not yet run and deletes them if they have already run. After calling, this
result and all dependent results will no longer be accessible.
"""
function cancel(client::Client, ops::Array{Dispatcher.Op})
    error("`cancel` not implemented yet")
end

"""
    gather(client::Client, ops::Array{Dispatcher.Op})

Gather the results of all `ops`. Requires there to be at least one worker
available to the scheduler or hangs indefinetely waiting for the results.
"""
function gather(client::Client, ops::Array{Dispatcher.Op})
    if client.status ∉ SHUTDOWN
        send_to_scheduler(
            client,
            Dict(
                "op" => "client-desires-keys",
                "keys" => [to_key(get_key(op)) for op in ops],
                "client" => client.id
            )
        )
    end
    results = []
    for op in ops
        push!(results, result(client, op))
    end
    return results
end

"""
    shutdown(client::Client)

Tell the dask-scheduler to terminate idle workers and that this client is shutting down.
Does NOT terminate the scheduler itself. This does not have to be called after a session
but is useful when you want to delete all the information submitted by the client from
the scheduler and workers (such as between test runs). If you want to reconnect to the
scheduler after calling this function you will have to set up a new client.
"""
function shutdown(client::Client)
    if client.status ∉ SHUTDOWN
        client.status = "closing"

        # Tell scheduler to close idle workers
        response = send_recv(
            client.scheduler,
            Dict("op" => "retire_workers", "close_workers" => true)
        )
        if !isempty(response)
            if isa(response, String)
                response = [response]
            end
            info(logger, "Closed $(length(response)) workers at: $response")
        end

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
        # TODO: throw more appropriate error types
        error(
            "No clients found\n" *
            "Start a client and point it to the scheduler address\n" *
            "    client = Client(\"ip-addr-of-scheduler:8786\")\n"
        )
    end
end

"""
    get_key(op::Dispatcher.Op)

Calculate an identifying key for `op`. Keys are re-used for identical `ops` to avoid
unnecessary computations.
"""
function get_key(op::Dispatcher.Op)
    return string(get_label(op), "-", hash((op.func, op.args, op.kwargs)))
end

##############################     INTERNAL USE FUNCTIONS     ##############################

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
            comm = connect(
                TCPSocket(),
                client.scheduler_address.host,
                client.scheduler_address.port
            )
            response = send_recv(
                comm,
                Dict("op" => "register-client", "client" => client.id, "reply"=> false)
            )

            try
                @assert length(response) == 1
                @assert response["op"] == "stream-start"
            catch
                error(
                    "An error ocurred on the dask-scheduler while registering this client."
                )
            end

            client.scheduler_comm = BatchedSend(comm, interval=0.01)
            client.connecting_to_scheduler = false

            push!(global_client, client)
            client.status = "running"

            while !isempty(client.pending_msg_buffer)
                send_to_scheduler(client, pop!(client.pending_msg_buffer))
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









