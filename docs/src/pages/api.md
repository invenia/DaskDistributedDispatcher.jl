# API

## Client

```@docs
Client
Client(::String)
submit(::Client, ::DispatchNode; ::Array)
result(::Client, ::DispatchNode)
cancel{T<:DispatchNode}(::Client, ::Array{T, 1})
gather{T<:DispatchNode}(::Client, ::Array{T, 1})
shutdown(::Client)
default_client()
get_key(::DispatchNode)
```

## Worker

```@docs
Worker
Worker(::String)
shutdown(::Array{Address, 1})
show(::IO, ::Worker)
```

## Server

```@docs
Server
start_listening(::Server)
```

## Address

```@docs
Address
Address(::String)
Address(::Union{IPAddr, String}, ::Integer)
```
