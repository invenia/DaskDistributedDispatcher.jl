# TODO: documentation, tests, shutdown


##############################             RPC                ##############################

type Rpc
    sockets::Dict{TCPSocket, Bool}
    address::URI
    shutdown::Bool
end

Rpc(address::URI) = Rpc(Dict{TCPSocket, Bool}(), address, false)

send_recv(rpc::Rpc, msg::Dict) = send_recv(get_comm(rpc), msg)

function start_comm(rpc::Rpc)
    sock = connect(TCPSocket(), rpc.address.host, rpc.address.port)
    push!(rpc.sockets, (sock => true))
    return sock
end

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

type ConnectionPool
    num_open::Integer
    num_active::Integer
    num_limit::Integer
    available::Dict{String, Set}
    occupied::Dict{String, Set}
end

function ConnectionPool(limit::Integer=512)
    ConnectionPool(0, 0, limit, Dict{String, Set}(), Dict{String, Set}())
end

function Base.show(io::IO, pool::ConnectionPool)
    print(io, "<ConnectionPool: open=$(pool.num_open), active=$(pool.num_active)>")
end

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
Get a Comm to the given address.
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
            # self.event.clear()
            collect(pool)
            # yield self.event.wait()
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


