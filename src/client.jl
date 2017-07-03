global_client = []

"""
    Client

A `Client` represents a client that the user can interact with to submit computations to
the scheduler and gather results.
"""
type Client
    ops::Dict{String, Dispatcher.Op}
    id::String
    status::String
    scheduler_address::URI
    # scheduler handles the TCP connections for all other messages such as "gather", etc
    scheduler::Rpc
    # scheduler_comm is used for the following messages to the scheduler:
    # "register-clinet", "update-graph", "client-desires-keys", "update-data", "report-key",
    # "client-releases-keys", "restart"
    connecting_to_scheduler::Bool
    scheduler_comm::Nullable{BatchedSend}
    connection_pool::ConnectionPool
    pending_msg_buffer::Array  # Used by scheduler_comm
end

"""
    Client(scheduler_address::String) -> Client

Construct a `Client` which can then be used to submit computations or gather results from
the dask-scheduler process.
"""
function Client(scheduler_address::String)
    scheduler_address = build_URI(scheduler_address)
    client = Client(
        Dict{String, Dispatcher.Op}(),
        "$(Base.Random.uuid1())",
        "connecting",
        scheduler_address,
        Rpc(scheduler_address),
        false,
        nothing,
        ConnectionPool(),
        [],
    )
    ensure_connected(client)
    return client
end

"""
    submit(client::Client, op::Dispatcher.Op; workers::Array=[])

Submit the `Op` computation unit to the dask-scheduler for computation.
"""
function submit(client::Client, op::Dispatcher.Op; workers::Array=[])
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
    if haskey(client.ops, key)
        result = fetch(client.ops[key])
        # Resuse a previously computed value if possible
        if client.ops[key] != op
            put!(op.result, result)
        end
    else
        error("The client does not have the requested op: $op")
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
    results = []
    for op in ops
        push!(results, result(client, op))
    end
    return results
end

"""
    shutdown(client::Client)

Tell the dask-scheduler to close all workers and that this client is shutting down.
"""
function shutdown(client::Client)
    send_to_scheduler(client, Dict("op" => "retire_workers", "close_workers" => true))
    send_to_scheduler(client, Dict("op" => "close-stream"))

    if !isempty(global_client) && global_client[1] == client
        pop!(global_client)
    end

    # This should terminate the scheduler but doesn't work properly
    # send_to_scheduler(client, Dict("op" => "close", "reply" => false))
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
            "Start an client and point it to the scheduler address\n" *
            "  from distributed import Client\n" *
            "  client = Client('ip-addr-of-scheduler:8786')\n"
        )
    end
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
        error("Client not running. Status: $(client.status)")
    end
end

"""
    gather_from_workers(client::Client, who_has::Dict)

Gather data directly from `who_has` peers.
"""
function gather_from_workers(client::Client, who_has::Dict)
    @async begin
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
                notice(logger, "addresses: $addresses")
                if haskey(results, key)
                    continue
                elseif isempty(addresses)
                    push!(all_bad_keys, key)
                    continue
                end
                try
                    possible_addresses = collect(setdiff(addresses, bad_addresses))
                    if isempty(possible_addresses)
                        push!(all_bad_keys, key)
                        continue
                    end
                    address = rand(possible_addresses)
                    !haskey(directory, address) && (directory[address] = [])
                    push!(directory[address], key)
                    rev[key] = address
                catch exception
                    rethrow(exception)
                # except IndexError:
                #     bad_keys.add(key)
                end
            end
            !isempty(bad_keys) && union!(all_bad_keys, bad_keys)

            responses = Dict()
            try
                for (address, keys_to_gather) in directory
                    try
                        response = send_recv(
                            client.connection_pool,
                            address,
                            Dict(
                                "op" => "get_data",
                                # TODO: these keys need to be encoded if they are sent to sccheduelr
                                "keys" => keys_to_gather,
                                "close" => false,
                            ),
                        )
                    catch exception
                        rethrow(exception)  # TODO: only catch EnvironmentError
                        push!(missing_workers, address)
                    finally
                        responses[address] = response
                    end
                end
            end

            union!(bad_addresses, Set(v for (k, v) in rev if !haskey(responses, k)))
            merge!(results, responses)
        end

        bad_keys = Dict(k => collect(original_who_has[k]) for k in all_bad_keys)

        return results, bad_keys, collect(missing_workers)
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







