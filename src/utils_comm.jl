const collection_types = (AbstractArray, Base.AbstractSet, Tuple)

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
        send_msg(comm, Dict("op" => "close", "reply" => false))
        close(comm)
    catch exception
        warn(logger,  "An error occured while closing connection: $exception")
    end
end

"""
    read_msg(msg)

Convert `msg` from bytes to strings except for serialized parts.
"""
function read_msg(msg)
    if isa(msg, Array{UInt8, 1})
        result = convert(String, msg)
        if !isvalid(String, result)
            result = msg
        end
        return result
    elseif isa(msg, Pair)
        return (read_msg(msg.first) => read_msg(msg.second))
    elseif isa(msg, Dict)
        return Dict(read_msg(kv) for (kv) in msg)
    elseif any(collection_type -> isa(msg, collection_type), collection_types)
         return map(x -> read_msg(x), msg)
    else
        return string(msg)
    end
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
    validate_key(key)

Validate a key as received on a stream.
"""
function validate_key(key)
    if !isa(key, String)
        error("Unexpected key type $(typeof(key)) (value: $key)")
    end
end

"""
    pack_data(object::Any, data::Dict; key_types::Type=String)

Merge known `data` into `object`.
"""
function pack_data(object::Any, data::Dict; key_types::Type=String)
    if any(t -> isa(object, t), collection_types)
        return map(x -> pack_object(x, data, key_types=key_types), object)
    elseif isa(object, Dict)
        return Dict(k => pack_object(v, data, key_types=key_types) for (k,v) in object)
    else
        pack_object(object)
    end
end

"""
    pack_object(object::Any, data::Dict; key_types::Type=key_types)

Replaces a Dispatcher.Op's key with its result if `object` is a known key.
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
    if any(t -> isa(object, t), collection_types)
        return map(item -> unpack_object(item), object)
    elseif isa(object, Dict)
        return Dict(unpack_object(k) => unpack_object(v) for (k,v) in object)
    else
        unpack_object(object)
    end
end

"""
    unpack_object(object::Any)

Replaces `object` with its key if `object` is a Dispatcher.Op. Otherwise returns the
original `object`.
"""
function unpack_object(object::Any)
    if isa(object, Dispatcher.Op)
        return get_key(object)
    else
        return object
    end
end


# Sources used: https://gist.github.com/shashi/e8f37c5f61bab4219555cd3c4fef1dc4
