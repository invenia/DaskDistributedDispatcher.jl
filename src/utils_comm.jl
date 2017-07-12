const collection_type = Union{AbstractArray, Base.AbstractSet, Tuple}

"""
    send_recv(sock::TCPSocket, msg::Dict)

Send a message and wait for the response.
"""
function send_recv(sock::TCPSocket, msg::Dict)
    send_msg(sock, msg)
    response = recv_msg(sock)

    # Get rid of unnecessary array wrapper that the scheduler sometimes sends
    if isa(response, Array) && length(response) == 1
        response = response[1]
    end
    return response
end

"""
    send_msg(sock::TCPSocket, msg::Dict)

Send `msg` to `sock` serialized by MsgPack following the dask.distributed protocol.
"""
function send_msg(sock::TCPSocket, msg::Union{Dict, Array, String})
    header = Dict()
    messages = [header, msg]
    frames = [MsgPack.pack(msg) for msg in messages]

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
    header, byte_msg = map(x->!isempty(x) ? MsgPack.unpack(x) : Dict(), frames)
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
    catch exception
        info(logger, "An error ocurred while closing connection: $exception")
    finally
        close(comm)
    end
end

"""
    read_msg(msg)

Convert `msg` from bytes to strings except for serialized parts.
"""
function read_msg(msg::Any)
    return string(msg)
end

function read_msg(msg::Array{UInt8, 1})
    result = convert(String, msg)
    if !isvalid(String, result)
        result = msg
    end
    return result
end

function read_msg(msg::collection_type)
    return map(x -> read_msg(x), msg)
end

function read_msg(msg::Dict)
    return Dict(read_msg(k) => read_msg(v) for (k,v) in msg)
end

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
    pack_data(object::collection_type, data::Dict; key_types::Type=String)

Merge known `data` into `object` if `object` is a collection type.
"""
function pack_data(object::collection_type, data::Dict; key_types::Type=String)
    return map(x -> pack_object(x, data, key_types=key_types), object)
end

"""
    pack_data(object::Dict, data::Dict; key_types::Type=String)

Merge known `data` into `object` if `object` is a dictionary type.
"""
function pack_data(object::Dict, data::Dict; key_types::Type=String)
    return Dict(k => pack_object(v, data, key_types=key_types) for (k,v) in object)
end

"""
    pack_object(object::Any, data::Dict; key_types::Type=key_types)

Replace a Dispatcher.Op's key with its result only if `object` is a known key.
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

Unpack `Dispatcher.Op` objects from `object`. Returns the unpacked object.
"""
function unpack_data(object::Any)
    return unpack_object(object)
end

function unpack_data(object::collection_type)
    return map(item -> unpack_object(item), object)
end

function unpack_data(object::Dict)
    return Dict(unpack_object(k) => unpack_object(v) for (k,v) in object)
end

"""
    unpack_object(object::Dispatcher.Op)

Replace `object` with its key if `object` is a Dispatcher.Op or else returns the original
`object`.
"""
function unpack_object(object::Any)
    return object
end

function unpack_object(object::Dispatcher.Op)
    return get_key(object)
end

# Sources used: https://gist.github.com/shashi/e8f37c5f61bab4219555cd3c4fef1dc4
