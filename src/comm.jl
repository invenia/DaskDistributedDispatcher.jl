##############################             RPC                ##############################

"""
    Rpc

Manage open socket connections to a specific address.
"""
type Rpc
    sockets::Array{TCPSocket, 1}
    address::URI
end

"""
    Rpc(address::URI) -> Rpc

Manage, open, and reuse socket connections to a specific address as required.
"""
Rpc(address::URI) = Rpc(Array{TCPSocket, 1}(), address)

"""
    Base.show(io::IO, rpc::Rpc)

Print a representation of the `Rpc` to `io`.
"""
function Base.show(io::IO, rpc::Rpc)
    print(
        io,
        "<Rpc: connected to=$(rpc.address), number of sockets open=$(length(rpc.sockets))>"
    )
end

"""
    send_recv(rpc::Rpc, msg::Dict) -> Dict

Send `msg` and wait for a response.
"""
function send_recv(rpc::Rpc, msg::Dict)
    comm = get_comm(rpc)
    response = send_recv(comm, msg)
    push!(rpc.sockets, comm)  # Mark as not in use
    return response
end

"""
    send_msg(rpc::Rpc, msg::Dict)

Send a `msg`.
"""
function send_msg(rpc::Rpc, msg::Dict)
    comm = get_comm(rpc)
    send(comm, msg)
    push!(rpc.sockets, comm)  # Mark as not in use
end


"""
    start_comm(rpc::Rpc) -> TCPSocket

Start a new socket connection.
"""
function start_comm(rpc::Rpc)
    sock = connect(TCPSocket(), rpc.address.host, rpc.address.port)
    return sock
end

"""
    get_comm(rpc::Rpc) -> TCPSocket

Reuse a previously open connection if available, if not, start a new one.
"""
function get_comm(rpc::Rpc)
    # Get rid of closed sockets
    filter!(sock -> isopen(sock), rpc.sockets)

    # Reuse sockets no longer in use
    sock = !isempty(rpc.sockets) ? pop!(rpc.sockets) : start_comm(rpc)
    return sock
end

"""
    Base.close(rpc::Rpc)

Close all communications.
"""
function Base.close(rpc::Rpc)
    for comm in rpc.sockets
        close(comm)
    end
end

##############################      CONNECTION POOL           ##############################

"""
    ConnectionPool

Manage a limited number pool of TCPSocket connections to different addresses.
Default number of open connections allowed is 512.
"""
type ConnectionPool
    num_open::Integer
    num_active::Integer
    num_limit::Integer
    available::Dict{String, Set}
    occupied::Dict{String, Set}
end

"""
    ConnectionPool(limit::Integer=512) -> ConnectionPool

Return a new `ConnectionPool` which limits the total possible number of connections open
to `limit`.
"""
function ConnectionPool(limit::Integer=512)
    ConnectionPool(0, 0, limit, Dict{String, Set}(), Dict{String, Set}())
end

"""
    Base.show(io::IO, pool::ConnectionPool)

Print a representation of the `ConnectionPool` to `io`.
"""
function Base.show(io::IO, pool::ConnectionPool)
    print(io, "<ConnectionPool: open=$(pool.num_open), active=$(pool.num_active)>")
end

"""
    send_recv(pool::ConnectionPool, address::String, msg::Dict) -> Dict

Send `msg` to `address` and wait for a response.
"""
function send_recv(pool::ConnectionPool, address::String, msg::Dict)

    debug(logger, "sending and recieving $msg to $address")
    comm = get_comm(pool, address)
    response = Dict()
    try
        response = send_recv(get_comm(rpc), msg)
    finally
        reuse(pool, address, comm)
    end
    return response
end

"""
    get_comm(pool::ConnectionPool, address::String)

Get a TCPSocket connection to the given address.
"""
function get_comm(pool::ConnectionPool, address::String)
    @async begin
        available = get!(pool.available, address, Set())
        occupied = get!(pool.occupied, address, Set())

        if !isempty(available)
            sock = pop!(pool.available)
            if isopen(comm)
                pool.num_active += 1
                push!(occupied, comm)
                return comm
            else
                pool.num_open -= 1
            end
        end

        while pool.num_open >= pool.num_limit
            collect_comms(pool)
        end

        pool.num_open += 1

        address = build_URI(address)
        comm = connect(address.host, address.port)

        if !isopen(comm)
            pool.num_open -= 1
            throw("Comm was closed")  # TODO: throw a better error
        end
        pool.num_active += 1
        push!(occupied, comm)

        return comm
    end
end

"""
    reuse(pool::ConnectionPool, address::String, comm::TCPSocket)

Reuse an open communication to the given address.
"""
function reuse(pool::ConnectionPool, address::String, comm::TCPSocket)
    delete!(pool.occupied[addr], comm)
    pool.num_active -= 1
    if !isopen(comm)
        pool.num_open -= 1
    else
        push!(pool.available[addr], comm)
    end
end

"""
    collect_comms(pool::ConnectionPool)

Collect open but unused communications to allow opening other ones.
"""
function collect_comms(pool::ConnectionPool)
    info(
        logger,
        "Collecting unused comms.  open: $(pool.num_open), active: $(pool.num_active)"
    )
    for (address, comms) in pool.available
        for comm in comms
            close(comm)
        end
        empty!(comms)
    end

    pool.num_open = pool.num_active
end

"""
    Base.close(pool::ConnectionPool)

Close all communications.
"""
function Base.close(pool::ConnectionPool)
    for comms in values(pool.available)
        for comm in comms
            close(comm)
        end
    end
    for comms in values(pool.occupied)
        for comm in comms
            close(comm)
        end
    end
end


##############################          BATCHED SEND          ##############################

"""
    BatchedSend

Batch messages in batches on a stream. Batching several messages at once helps performance
when sending a myriad of tiny messages. Used by both the julia worker and client to
communicate with the scheduler.
"""
type BatchedSend
    interval::AbstractFloat
    please_stop::Bool
    buffer::Array{Dict{String, Any}}
    comm::TCPSocket
    next_deadline::Nullable{AbstractFloat}
end

"""
    BatchedSend(comm::TCPSocket; interval::AbstractFloat=0.002) -> BatchedSend

Batch messages in batches on `comm`. We send lists of messages every `interval`
milliseconds.
"""
function BatchedSend(comm::TCPSocket; interval::AbstractFloat=0.002)
    batchedsend = BatchedSend(
        interval,
        false,
        Array{Dict{String, Any}, 1}(),
        comm,
        nothing
    )
    background_send(batchedsend)
    return batchedsend
end

"""
    Base.show(io::IO, batchedsend::BatchedSend)

Print a representation of the `BatchedSend` to `io`.
"""
function Base.show(io::IO, batchedsend::BatchedSend)
    print(io, "<BatchedSend: $(length(batchedsend.buffer)) in buffer>")
end

"""
    background_send(batchedsend::BatchedSend)

Send the messages in `batchsend.buffer` every `interval` milliseconds.
"""
function background_send(batchedsend::BatchedSend)
    @async while !batchedsend.please_stop
        if isempty(batchedsend.buffer)
            batchedsend.next_deadline = nothing
            sleep(batchedsend.interval/2)
            continue
        end

        if isnull(batchedsend.next_deadline)
            sleep(batchedsend.interval/2)
            continue
        end

        # Wait until the next deadline to send messages
        if time() < get(batchedsend.next_deadline)
            continue
        end

        payload, batchedsend.buffer = batchedsend.buffer, Array{Dict{String, Any}, 1}()
        send_msg(batchedsend.comm, payload)
        sleep(batchedsend.interval/2)
    end
end

"""
    send_msg(batchedsend::BatchedSend, msg::Dict{String, Any})

Schedule a message for sending to the other side. This completes quickly and synchronously.
"""
function send_msg(batchedsend::BatchedSend, msg::Dict)
    push!(batchedsend.buffer, msg)
    if isnull(batchedsend.next_deadline)
        batchedsend.next_deadline = time() + batchedsend.interval
    end
end

"""
    Base.close(batchedsend::BatchedSend)

Try to send all remaining messages and then close the connection.
"""
function Base.close(batchedsend::BatchedSend)
    batchedsend.please_stop = true
    if isopen(batchedsend.comm)
        if !isempty(batchedsend)
            payload, batchedsend.buffer = batchedsend.buffer, Array{Dict{String, Any}, 1}()
            send_msg(batchedsend.comm, payload)
        end
    end
    close(batchedsend.comm)
end

"""
    abort(batchedsend::BatchedSend)

Immediately close the connection.
"""
function abort(batchedsend::BatchedSend)
    batchedsend.please_stop = true
    batchedsend.buffer = Array{Dict{String, Any}}()
    if isopen(batchedsend.comm)
        close(batchedsend.comm)
    end
end


