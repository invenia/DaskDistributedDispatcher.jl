"""
    parse_address(address::String) -> (IpAddr, port, String)

Parses an address into its scheme, host, and port components.
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
    if host_and_port[1] != ""
        host = parse(IPAddr, host_and_port[1])
    else
        error("Cannot extract host from address: \"$address\"")
    end

    if length(host_and_port) > 1 && host_and_port[2] != ""
        port = parse(Int64, host_and_port[2])
    else
        port = 0
    end

    @assert isa(host, IPAddr)
    @assert isa(port, Integer) && port >= 0
    @assert isa(scheme, String)

    return host, port, scheme
end

"""
    format_URI(scheme::String="tcp", host::IPAddr, port::Integer) -> String

Build a string represntation of an URI out of its scheme, host, and port components.
"""
function format_URI{T1<:IPAddr, T2<:Integer}(host::T1, port::T2, scheme::String="tcp")
    return "$scheme://$(string(host)):$port"
end

"""
    build_URI(scheme::String="tcp", host::IPAddr, port::Integer) -> URI

Build an URI out of its scheme, host, and port components.
"""
function build_URI{T1<:IPAddr, T2<:Integer}(host::T1, port::T2, scheme::String="tcp")
    return URI(format_URI(host, port, scheme))
end

"""
    build_URI(address::String) -> URI

Build an URI out of an unsanitized string input.
"""
function build_URI(address::String)
    host, port, scheme = parse_address(address)
    return URI(format_URI(host, port, scheme))
end
