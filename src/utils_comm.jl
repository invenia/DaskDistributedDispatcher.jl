const CollectionType = Union{AbstractArray, Base.AbstractSet, Tuple}

"""
    send_recv{T<:Any}(sock::TCPSocket, msg::Dict{String, T})

Send a message and wait for the response.
"""
function send_recv{T<:Any}(sock::TCPSocket, msg::Dict{String, T})
    send_msg(sock, msg)
    response = recv_msg(sock)

    # Get rid of unnecessary array wrapper that the scheduler sometimes sends
    if isa(response, Array) && length(response) == 1
        response = response[1]
    end
    return response
end

"""
    send_msg(sock::TCPSocket, msg::Union{String, Array, Dict})

Send `msg` to `sock` serialized by MsgPack following the dask.distributed protocol.
"""
function send_msg(sock::TCPSocket, msg::Union{String, Array, Dict})
    header = Dict{Void, Void}()
    messages = [header, msg]
    frames = Vector{UInt8}[pack(msg) for msg in messages]

    isopen(sock) && write(sock, convert(UInt64, length(frames)))
    for frame in frames
        isopen(sock) && write(sock, convert(UInt64, length(frame)))
    end
    for frame in frames
        isopen(sock) && write(sock, frame)
    end

    # 8 bytes -> # of frames
    # N * 8 bytes -> lengths
    # blobs (1st one is MsgPack - specific to the protocol)
end

"""
    recv_msg(sock::TCPSocket) -> Union{String, Array, Dict}

Recieve `msg` from `sock` and deserialize it from msgpack encoded bytes to strings.
"""
function recv_msg(sock::TCPSocket)::Union{String, Array, Dict}
    num_frames = read(sock, UInt64)
    frame_lengths = UInt64[read(sock, UInt64) for i in 1:num_frames]
    frames = Vector{UInt8}[read(sock, length) for length in frame_lengths]
    header, byte_msg = map(x->!isempty(x) ? unpack(x) : nothing, frames)
    return read_msg(byte_msg)
end

"""
    close_comm(comm::TCPSocket)

Tell peer to close and then close the TCPSocket `comm`
"""
function close_comm(comm::TCPSocket)
    # Make sure we tell the peer to close
    try
        isopen(comm) && send_msg(
            comm,
            Dict{String, Union{String, Bool}}("op" => "close", "reply" => false)
        )
    finally
        close(comm)
    end
end

"""
    read_msg(msg::Any)

Convert `msg` from bytes to strings except for serialized parts.
"""
read_msg(msg::Any)::String = return string(msg)

function read_msg(msg::Vector{UInt8})::Union{String, Vector{UInt8}}
    result = convert(String, msg)
    if !isvalid(String, result)
        return msg
    end
    return result
end

read_msg(msg::CollectionType)::CollectionType = return map(read_msg, msg)

read_msg(msg::Dict)::Dict = return Dict(read_msg(k) => read_msg(v) for (k,v) in msg)

"""
    to_serialize(item) -> Vector{UInt8}

Serialize `item`.
"""
function to_serialize(item)::Vector{UInt8}
    io = IOBuffer()
    serialize(io, item)
    serialized_bytes = take!(io)
    close(io)
    return serialized_bytes
end

"""
    to_deserialize(serialized_item::Vector{UInt8}) -> Any

Parse and deserialize `serialized_item`.
"""
function to_deserialize(serialized_item::Vector{UInt8})
    io = IOBuffer()
    write(io, serialized_item)
    seekstart(io)
    item = deserialize(io)
    close(io)
    return item
end

"""
    pack_data(object::Any, data::Dict{String, Any})

Merge known `data` into `object`.
"""
pack_data(object::Any, data::Dict{String, Any}) = pack_object(object, data)

function pack_data(object::CollectionType, data::Dict{String, Any})
    return map(x -> pack_object(x, data), object)
end

function pack_data(object::Dict{String, Any}, data::Dict{String, Any})
    return Dict{String, Any}(k => pack_object(v, data) for (k,v) in object)
end

"""
    pack_object(object::Any, data::Dict{String, Any})

Replace a DispatchNode's key with its result only if `object` is a known key.
"""
pack_object(object::Any, data::Dict{String, Any}) = object

pack_object(object::String, data::Dict{String, Any}) = get(data, object, object)

"""
    unpack_data(object::Any)

Unpack `DispatchNode` objects from `object`. Returns the unpacked object.
"""
unpack_data(object::Any) = unpack_object(object)

unpack_data(object::CollectionType) = map(unpack_object, object)

unpack_data(object::Dict) = Dict(unpack_object(k) => unpack_object(v) for (k,v) in object)

"""
    unpack_object(object::Any)

Replace `object` with its key if `object` is a DispatchNode or else returns the original
`object`.
"""
unpack_object(object::Any)::Any = return object

unpack_object(object::DispatchNode)::String = return get_key(object)

# Sources used: https://gist.github.com/shashi/e8f37c5f61bab4219555cd3c4fef1dc4
