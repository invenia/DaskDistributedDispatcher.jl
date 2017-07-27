const CollectionType = Union{AbstractArray, Base.AbstractSet, Tuple}
const Message = Union{String, Array, Dict}

"""
    send_recv(sock::TCPSocket, msg::Dict)

Send a message and wait for the response.
"""
function send_recv(sock::TCPSocket, msg::Dict)
    send_msg(sock, msg)
    response = []
    if isopen(sock)
        response = recv_msg(sock)
    end

    # Get rid of unnecessary array wrapper that the scheduler sometimes sends
    if isa(response, Array) && length(response) == 1
        response = response[1]
    end
    return response
end

"""
    send_msg(sock::TCPSocket, msg::Message)

Send `msg` to `sock` serialized by MsgPack following the dask.distributed protocol.
"""
function send_msg(sock::TCPSocket, msg::Message)
    header = Dict()
    messages = [header, msg]
    frames = [pack(msg) for msg in messages]

    write(sock, convert(UInt64, length(frames)))
    for frame in frames
        write(sock, convert(UInt64, length(frame)))
    end
    for frame in frames
        write(sock, frame)
    end

    # 8 bytes -> # of frames
    # N * 8 bytes -> lengths
    # blobs (1st one is MsgPack - specific to the protocol)
end

"""
    recv_msg(sock::TCPSocket) -> Union{Array, Dict}

Recieve `msg` from `sock` and deserialize it from msgpack encoded bytes to strings.
"""
function recv_msg(sock::TCPSocket)
    num_frames = read(sock, UInt64)
    frame_lengths = [read(sock, UInt64) for i in 1:num_frames]
    frames = [read(sock, length) for length in frame_lengths]
    header, byte_msg = map(x->!isempty(x) ? unpack(x) : Dict(), frames)
    return read_msg(byte_msg)
end

"""
    close_comm(comm::TCPSocket)

Tell peer to close and then close the TCPSocket `comm`
"""
function close_comm(comm::TCPSocket)
    # Make sure we tell the peer to close
    try
        if isopen(comm)
            send_msg(comm, Dict("op" => "close", "reply" => false))
        end
    finally
        close(comm)
    end
end

"""
    read_msg(msg::Any)

Convert `msg` from bytes to strings except for serialized parts.
"""
read_msg(msg::Any) = return string(msg)

function read_msg(msg::Array{UInt8, 1})
    result = convert(String, msg)
    if !isvalid(String, result)
        result = msg
    end
    return result
end

read_msg(msg::CollectionType) = return map(read_msg, msg)

read_msg(msg::Dict) = return Dict(read_msg(k) => read_msg(v) for (k,v) in msg)

"""
    to_serialize(item)

Serialize `item` if possible, otherwise convert to format that can be encoded by msgpack.
"""
function to_serialize(item)
    io = IOBuffer()
    serialize(io, item)
    serialized_bytes = take!(io)
    close(io)
    return serialized_bytes
end

"""
    to_deserialize(item)

Parse and deserialize `item`.
"""
function to_deserialize(serialized_item)
    io = IOBuffer()
    write(io, serialized_item)
    seekstart(io)
    item = deserialize(io)
    close(io)
    return item
end

"""
    to_key(key::String)

Convert a key to a non-unicode string so that the dask-scheduler can work with it.
"""
to_key(key::String) = return transcode(UInt8, key)

"""
    pack_data(object::Any, data::Dict; key_types::Type=String)

Merge known `data` into `object`.
"""
function pack_data(object::Any, data::Dict; key_types::Type=String)
    return pack_object(object, data, key_types=key_types)
end

function pack_data(object::CollectionType, data::Dict; key_types::Type=String)
    return map(x -> pack_object(x, data, key_types=key_types), object)
end

function pack_data(object::Dict, data::Dict; key_types::Type=String)
    return Dict(k => pack_object(v, data, key_types=key_types) for (k,v) in object)
end

"""
    pack_object(object::Any, data::Dict; key_types::Type=key_types)

Replace a DispatchNode's key with its result only if `object` is a known key.
"""
function pack_object(object::Any, data::Dict; key_types::Type=key_types)
    if isa(object, key_types) && haskey(data, object)
        return data[object]
    else
        return object
    end
end

"""
    unpack_data(object::Any)

Unpack `DispatchNode` objects from `object`. Returns the unpacked object.
"""
unpack_data(object::Any) = return unpack_object(object)

unpack_data(object::CollectionType) = return map(unpack_object, object)

function unpack_data(object::Dict)
    return Dict(unpack_object(k) => unpack_object(v) for (k,v) in object)
end

"""
    unpack_object(object::Any)

Replace `object` with its key if `object` is a DispatchNode or else returns the original
`object`.
"""
unpack_object(object::Any) = return object

unpack_object(object::DispatchNode) = return get_key(object)

# Sources used: https://gist.github.com/shashi/e8f37c5f61bab4219555cd3c4fef1dc4
