"""
    Address

A representation of an endpoint that can be connected to. It is categorized by its scheme
(tcp is currently the only protocol supported), host, and port.
"""
@auto_hash_equals type Address
    scheme::String
    host::IPAddr
    port::Integer
end

"""
    Address(address::String) -> Address

Parse `address` and returns the corresponding `Address` object.
"""
function Address(address::String)
    Address(parse_address(address)...)
end

"""
    Address(host::IPAddr, port::Integer)) -> Address

Return the corresponding `Address` object to the components `host` and `port`. By default
the tcp protocol is assumed.
"""
function Address(host::IPAddr, port::Integer)
    scheme = "tcp"
    if host == ip"127.0.0.1"
        host = getipaddr()
    end

    @assert port >= 0
    Address(scheme, host, port)
end

"""
    show(io::IO, address::Address)

Print a representation of the address to `io`. The format used to represent addresses is
"tcp://127.0.0.1:port".
"""
function Base.show(io::IO, address::Address)
    print(io, "$(address.scheme)://$(string(address.host)):$(address.port)")
end

"""
    Base.connect(address::Address)

Open a tcp connection to `address`.
"""
Base.connect(address::Address) = return connect(address.host, address.port)

"""
    MsgPack.pack(io::Base.AbstractIOBuffer{Array{UInt8,1}}, address::Address)

Pack `address` as its string representation.
"""
function MsgPack.pack(io::Base.AbstractIOBuffer{Array{UInt8,1}}, address::Address)
    return pack(io, string(address))
end

"""
    parse_address(address::String) -> (String, IpAddr, Integer)

Parse an address into its scheme, host, and port components.
"""
function parse_address(address::String)
    scheme = "tcp"
    address = replace(address, r"(.*://)", "")

    host_and_port = Array{String}(split(address, ':'))
    host = host_and_port[1] == "127.0.0.1" ? getipaddr() : parse(IPAddr, host_and_port[1])

    if length(host_and_port) > 1 && host_and_port[2] != ""
        port = parse(Int64, host_and_port[2])
    else
        port = 0
    end

    return scheme, host, port
end
