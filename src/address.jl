"""
    Address

A representation of an endpoint that can be connected to. It is categorized by its scheme
(tcp is currently the only protocol supported), host, and port.
"""
type Address
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
    Address(address::String) -> Address

Return the corresponding `Address` object to the components `host` and `port`. By default
the tcp protocol is assumed.
"""
function Address(host::Union{IPAddr, String}, port::Integer)
    scheme = "tcp"
    if string(host) == "127.0.0.1"
        host = getipaddr()
    elseif !isa(host, IPAddr)
        host = parse(IPAddr, host)
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
    ==(x::Address, y::Address)

Return true if `x` and `y` are equal to each other (have all the same fields even if they
are not the same object).
"""
function ==(x::Address, y::Address)
    return x.scheme == y.scheme && string(x.host) == string(y.host) && x.port == y.port
end

"""
    Base.hash(address::Address)

Compute an integer hash code such that any `Address` with the same fields will be equal to
each other.
"""
function Base.hash(address::Address)
    return hash((address.scheme, address.host, address.port))
end

"""
    Base.connect(address::Address)

Open a tcp connection to `address`.
"""
function Base.connect(address::Address)
    return connect(address.host, address.port)
end

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
    address_elements = Array{String}(split(address, "://"))

    if length(address_elements) > 1
        scheme = address_elements[1]
        address = address_elements[2]
    else
        scheme = "tcp"
        address = address_elements[1]
    end

    host_and_port = Array{String}(split(address, ':'))
    host = ""
    try
        if host_and_port[1] == "127.0.0.1"
            host = getipaddr()
        else
            host = parse(IPAddr, host_and_port[1])
        end
    catch
        error("Could not extract host from address: \"$address\"")
    end

    if length(host_and_port) > 1 && host_and_port[2] != ""
        port = parse(Int64, host_and_port[2])
    else
        port = 0
    end

    @assert isa(host, IPAddr)
    @assert isa(port, Integer) && port >= 0
    @assert isa(scheme, String)

    return scheme, host, port
end
