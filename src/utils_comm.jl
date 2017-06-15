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
function send_msg(sock::TCPSocket, msg::Dict)
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
    recv_msg(sock::TCPSocket)

Recieve `msg` from `sock` and deserialize it from msgpack encoded bytes to strings.
"""
function recv_msg(sock::TCPSocket)
    num_frames = read(sock, UInt64)
    frame_lengths = [read(sock, UInt64) for i in 1:num_frames]
    frames = [read(sock, length) for length in frame_lengths]
    header, hex_msg = map(x->!isempty(x) ? MsgPack.unpack(x) : Dict(), frames)

    msg = read_msg(hex_msg)
    debug(logger, "Recieved parsed msg: $msg")
    return msg
end

"""
    read_msg(hex_msg)

Convert `msg` from bytes to strings.
"""
function read_msg(hex_msg)
    if isa(hex_msg, Array{UInt8, 1})
        result = convert(String, hex_msg)
        if !isvalid(String, result)
            msg = ""
            for i in eachindex(hex_msg)
                char = convert(Char, hex_msg[i])
                msg = string(msg, char)
            end
            result = msg
        end
        return result
    elseif isa(hex_msg, Pair)
        return Pair(read_msg(hex_msg.first), read_msg(hex_msg.second))
    elseif isa(hex_msg, Dict) || isa(hex_msg, Array)  # TODO: simplify this like in to_deserialize
        if isa(hex_msg, Dict)
            msg = Dict()
        elseif isa(hex_msg, Array)
            msg = []
        end
        for item in hex_msg
            push!(msg, read_msg(item))
        end
        return msg
    else
        return string(hex_msg)
    end
end

"""
    to_serialize(item)

Serialize `item` if possible, otherwise convert to format that can be encoded by msgpack.
"""
function to_serialize(item)
    if isa(item, Integer) || isa(item, String)
        return item
    elseif isa(item, Type)
        return string(item)  # serialize does not support types
    elseif isa(item, Pair)
        return (to_serialize(item[1]) => to_serialize(item[2]))
    else
        return serialize(item)
    end
end

"""
    to_deserialize(item)

Parse and deserialize `item`.
"""
function to_deserialize(item)  #TODO: rename and do better once function serialization is being used
    if isa(item, Type) || isa(item, Function)
        return item
    elseif isa(item, Integer) || isa(item, AbstractFloat) || isa(item, String)
        return parse(item)
    elseif isa(item, Array) || isa(item, Tuple)
        return map(x -> to_deserialize(x), item)
    elseif isa(item, Dict)
        return Dict(to_deserialize(kv) for (kv) in item)
    elseif isa(item, Pair)
        return (to_deserialize(item[1]) => to_deserialize(item[2]))
    else
        # debug(logger, "item: $item")
        return deserialize(item)
    end
end

"""
    to_key(key::String)

Convert a key to a non-unicode string so that the dask-scheduler can work with it.
"""
to_key(key::String) = return transcode(UInt8, key)

"""
    is_task(x)

Verify that `x` is a  task (a tuple with a callable first argument).
"""
function is_task(x)
    return isa(x, Tuple) && isa(x[1], Base.Callable)  # TODO: Probably move to worker.jl where it is used
end

"""
    validate_key(key)

Validate a key as received on a stream.
"""
function validate_key(key)  # TODO: move
    if !isa(key, String)
        error("Unexpected key type $(typeof(key)) (value: $key)")
    end
end

# Sources used: https://gist.github.com/shashi/e8f37c5f61bab4219555cd3c4fef1dc4
