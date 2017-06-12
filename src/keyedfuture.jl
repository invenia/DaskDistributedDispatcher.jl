type FutureState
    future::Future  # TODO: is this needed?
    status::String
    result_type::Any
    exception::Nullable{String}
    traceback::Nullable{String}
end

"""A Future's internal state.

This is shared between all Futures with the same key and client.
"""
FutureState(future::Any) = FutureState(future, "pending", nothing, nothing, nothing)

function cancel(future_state::FutureState)
    future_state.status = "cancelled"
    # future_state.event.set()  # TODO
end

function finish(future_state::FutureState, result_type=nothing)
    future_state.status = "finished"
    # future_state.event.set()
    future_state.result_type = mytype
end

function lose(future_state::FutureState)
    future_state.status = "lost"
    # future_state.event.clear()
end

function set_error(future_state::FutureState, exception::String, traceback::String)
    future_state.status = "error"
    future_state.exception = exception
    future_state.traceback = traceback
    # future_state.event.set()
end

function Base.show(io::IO, future_state::FutureState)
    print(io, "<$(typeof(future_state).name.name): $(future_state.status)>")
end

type KeyedFuture
    key::String
    client::Client
    state::FutureState
end

# TODO: fix documentation
""" A remotely running computation

A Future is a local proxy to a result running on a remote worker.  A user
manages future objects in the local julia process to determine what
happens in the larger cluster.

Examples
--------

Futures typically emerge from Client computations

>>> my_future = client.submit(add, 1, 2)  # doctest: +SKIP

We can track the progress and results of a future

>>> my_future
<Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>

We can get the result or the exception and traceback from the future

>>> my_future.result()

See Also
--------
Client:  Creates futures
"""
function KeyedFuture(key::String, client::Client)
    if !haskey(client.futures, key)
        client.futures[key] = FutureState(Future())  # TODO => FIgure out the remoteref stuff
    end
    keyedfuture =  KeyedFuture(key, client, client.futures[key])

    # TODO: this is not necessary, add flag for when/if this should be done
    send_to_scheduler(
        keyedfuture.client,
        Dict(
            "op" => "client-desires-keys",
            "keys" => [to_key(key)],
            "client" => keyedfuture.client.id
        )
    )
     return keyedfuture
end

function Base.show(io::IO, keyedfuture::KeyedFuture)
    print(
        io,
        "<Future: status: $(keyedfuture.state.status), " *
        "type: $(keyedfuture.state.result_type), " *
        "key: $(keyedfuture.key)>"
    )
end

function result(keyedfuture::KeyedFuture)  # TODO: implement timmout?
    if keyedfuture.state.status == "error"
        rethrow(state)  # This is just a place holder for now
    elseif keyedfuture.state.status == "cancelled"
        throw(CancelledError(worker.key))  # This is just a place holder for now
    else
        value = @sync consume(gather(keyedfuture.client, [keyedfuture]))
        debug(logger, "the result is $value")
        return value
    end
end
