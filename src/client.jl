global_client = []

"""
    Client

A `Client` represents a client that the user can interact with to submit computations to
the scheduler and gather results.
"""
type Client
    futures::Dict
    id::String
    status::String
    scheduler_address::URI
    # scheduler handles the TCP connections for all other messages such as "gather", etc
    scheduler::Rpc
    # scheduler_comm is used for the following messages to the scheduler:
    # "register-clinet", "update-graph", "client-desires-keys", "update-data", "report-key",
    # "client-releases-keys", "restart"
    scheduler_comm::TCPSocket
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
        Dict(),
        "$(Base.Random.uuid1())",
        "connecting",
        scheduler_address,
        Rpc(scheduler_address),
        TCPSocket(),
        ConnectionPool(),
        [],
    )
    ensure_connected(client)
    return client
end

"""
    submit(client::Client, op::Dispatcher.Op)

Submit the `Op` computation unit to the dask-scheduler for computation.
"""
function submit(client::Client, op::Dispatcher.Op)
    key = string(get_label(op), "-", hash(op.result))

    if !haskey(client.futures, key)

        # TODO: Implement dependencies, priority, and use serialization
        # io = IOBuffer()
        # serialize(io, Base.remoteref_id(op.result.outer))
        # seekstart(io);

        # tasks = Dict(key => Dict("future" => string(Base.remoteref_id(op.result.outer))))
        # tasks = Dict(key => Dict("future" => key))

        tasks = Dict(key => Dict("function" => "$(op.func)", "args" => collect(op.args)))
        task_dependencies = Dict(key => [])
        priority = Dict(key => 0)

        scheduler_op = Dict(
            "op" => "update-graph",
            "tasks" => tasks,
            "dependencies" => task_dependencies,
            "keys" => [to_key(key)],
            "restrictions" => Dict(),
            "loose_restrictions" => [],
            "priority" => priority,
            "resources" => nothing
        )

        info(logger, scheduler_op)  # TODO: remove
        send_to_scheduler(client, scheduler_op)

        client.futures[key] = op
    end
end

"""
    result(client::Client, op::Dispatcher.Op)

Gather the result of the `Op` computation unit. Requires there to be at least one worker
available to the scheduler or hangs indefinetely.
"""
function result(client::Client, op::Dispatcher.Op)
    @sync consume(gather(client, [op]))
end

"""
    cancel(client::Client, futures::Array)

Cancel all `Op`s in `futures`. This stops future tasks from being scheduled
if they have not yet run and deletes them if they have already run. After calling, this
result and all dependent results will no longer be accessible.
"""
function cancel(client::Client, futures::Array)
    error("`cancel` not implemented yet")
end

"""
    gather(client::Client, futures::Array)

Gather the results of all `Op`s in `futures`. Requires there to be at least one worker
available to the scheduler or hangs indefinetely.
"""
function gather(client::Client, futures::Array)  # TODO: gather ops instead?
    @async begin
        keys_to_gather = [future.key for future in futures]

        # TODO: Implement waiting on futures (only query scheduler once they have completed)
        # [wait(future.future) for filter(future -> haskey(client.futures, future.key), futures)]

        failed = ("error", "cancelled")

        exceptions = Set()

        # TODO: bad_keys
        # bad_keys = Set()

        for key in keys_to_gather
            if !haskey(client.futures, key) || client.futures[key].status in failed
                push!(exceptions, key)
                st = client.futures[key]
                exception = st.exception
                traceback = st.traceback
                rethrow(exception)
            end
        end

        # response = nothing
        # while response == nothing
        #     @sync send_msg(
        #         client.scheduler_comm,
        #         Dict(
        #             "op" => "client-desires-keys",
        #             "keys" => [to_key(keys_to_gather[1])],
        #             "client" => client.id,
        #         )
        #     )

        #     if nb_available(client.scheduler_comm.buffer) > 0
        #         response = consume(recv_msg(client.scheduler_comm))
        #         debug(logger, "sent to scheduler, response $response")
        #     else
        #         sleep(0.1)
        #     end
        # end

        # keys = [k for k in keys if k not in bad_keys]

        if true  # TODO: only query workers after, first figure out waiting on futures
            who_has = send_recv(
                client.scheduler,
                Dict("op" => "who_has", "keys" => [to_key(k) for k in keys_to_gather])
            )


            data, missing_keys, missing_workers = consume(gather_from_workers(client, who_has))
            notice(logger, "lala stuff: $((data, missing_keys, missing_workers))")
            response = Dict("status" => "OK", "data" => data)

            if !isempty(missing_keys)  # TODO: do we want to query the scheduler instead?
                missing = [key for key in filter(key -> !haskey(data, key), keys_to_gather)]
                @assert collect(keys(missing_keys)) == missing
                response = send_recv(
                    client.scheduler,
                    Dict("op" => "gather", "keys" => [to_key(k) for k in missing])
                )
                if response["status"] == "OK"
                    debug(logger, "Response was ok! : $response")
                    merge!(response["data"], data)
                end
            end
        else
            response = send_recv(
                client.scheduler,
                Dict("op" => "gather", "keys" => [to_key(k) for k in keys_to_gather])
            )
        end

        warn(logger, "the response was: $response")


        if response["status"] == "error"
            errored_keys = response["keys"]
            warn(logger, "Couldn't gather keys: $(keys(errored_keys))")
            for (key, result) in errored_keys
                send_to_scheduler(client, Dict("op" => "report-key", "key" => key))
            end
            # for key in response["keys"]
            #     self.futures[key].event.clear()  # TODO: futures
            # end
        else
            @assert response["status"] == "OK"

            #TODO: simplify this
            data = Dict(parse(k) => to_deserialize(v) for (k,v) in response["data"])
            if length(data) == 1
                result = collect(values(data))[1]
            else
                result = values(data)
            end
            return result  # TODO: fix all this
        end
    end

    # if bad_data and errors == "skip" and isinstance(futures2, list):
    #     futures2 = [f for f in futures2 if f not in bad_data]

    # data = response["data"]
    # result = pack_data(futures2, merge(data, bad_data))
    # raise gen.Return(result)
    # end
end

"""
    shutdown(client::Client)

Tells the dask-scheduler to shutdown. This cancels all currently running tasks, clears the
state of the scheduler, and shuts down all workers and scheduler. You only need to call this
if you want to take down the distributed cluster.
"""
function shutdown(client::Client)
        # client.scheduler.retire_workers, close_workers=True

    send_to_scheduler(client, Dict("op" => "retire_workers", "close_workers" => true))

    send_to_scheduler(client, Dict("op" => "close", "reply" => false))
    if !isempty(global_client) && global_client[1] == client
        pop!(global_client)
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
        if client.scheduler_comm.status == Base.StatusInit || !isopen(client.scheduler_comm)
            client.scheduler_comm == connect(
                client.scheduler_comm,
                client.scheduler_address.host,
                client.scheduler_address.port
            )
            response = send_recv(
                client.scheduler_comm,
                Dict("op" => "register-client", "client" => client.id, "reply"=> false)
            )
            @assert length(response) == 1
            @assert response["op"] == "stream-start"

            # TODO: later converts to a batched communication.

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
        send_msg(client.scheduler_comm, msg)
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



