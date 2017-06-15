# TODO: documentation, tests, shutdown

##############################             RPC                ##############################

"""
    Rpc

Manage open socket connections to a specific address.
"""
type Rpc
    sockets::Dict{TCPSocket, Bool}
    address::URI
    shutdown::Bool
end

"""
    Rpc(address::URI)

Manage, open, and reuse socket connections to a specific address as required.
"""
Rpc(address::URI) = Rpc(Dict{TCPSocket, Bool}(), address, false)

"""
    send_recv(rpc::Rpc, msg::Dict)

Send `msg` and wait for a response.
"""
send_recv(rpc::Rpc, msg::Dict) = send_recv(get_comm(rpc), msg)

"""
    start_comm(rpc::Rpc)

Start a new socket connection.
"""
function start_comm(rpc::Rpc)
    sock = connect(TCPSocket(), rpc.address.host, rpc.address.port)
    push!(rpc.sockets, (sock => true))
    return sock
end

"""
    get_comm(rpc::Rpc)

Reuse a previously open connection if available, if not, start a new one.
"""
function get_comm(rpc::Rpc)
    # Get rid of closed sockets
    filter!((k, v) -> isopen(k), rpc.sockets)

    # Reuse sockets no longer in use
    unused = filter((k, v) -> v == false, rpc.sockets)
    sock = !isempty(unused) ? first(unused) : start_comm(rpc)
    rpc.sockets[sock] = true  # Mark as in use

    return sock
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
    send_recv(pool::ConnectionPool, address::String, msg::Dict)

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
    send_recv(pool::ConnectionPool, address::String, msg::Dict)

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
            collect(pool)
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
    Base.collect(pool::ConnectionPool)

Collect open but unused communications, to allow opening other ones.
"""
function Base.collect(pool::ConnectionPool)
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


