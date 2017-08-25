# API

## DaskExecutor

```@docs
DaskExecutor
DaskExecutor(::String)
reset!(::DaskExecutor)
Dispatcher.run_inner_node!(::DaskExecutor, ::Dispatcher.DispatchNode, ::Int)
DaskDistributedDispatcher.retries(::DaskExecutor)
DaskDistributedDispatcher.retry_on(::DaskExecutor)
DaskDistributedDispatcher.dispatch!(::DaskExecutor, ::Dispatcher.DispatchNode)
```

## Client

```@docs
Client
Client(::String)
submit(::Client, ::Dispatcher.DispatchNode)
cancel{T<:Dispatcher.DispatchNode}(::Client, ::Vector{T})
gather{T<:Dispatcher.DispatchNode}(::Client, ::Vector{T})
replicate{T<:Dispatcher.DispatchNode}(::Client)
shutdown(::Client)
get_key{T<:Dispatcher.DispatchNode}(node::T)
DaskDistributedDispatcher.ensure_connected(::Client)
DaskDistributedDispatcher.send_to_scheduler{T}(::Client, ::Dict{String, T})
DaskDistributedDispatcher.serialize_deps{T<:Dispatcher.DispatchNode}(::Client, ::Vector{T}, ::Vector{Vector{UInt8}}, ::Dict{Vector{UInt8}, Dict{String, Vector{UInt8}}}, ::Dict{Vector{UInt8}, Vector{Vector{UInt8}}})
DaskDistributedDispatcher.serialize_node(::Client, ::Dispatcher.DispatchNode)
DaskDistributedDispatcher.serialize_task{T<:Dispatcher.DispatchNode}(::Client, node::T, ::Vector{T})
```

## Address

```@docs
Address
Address(::String)
Address(::IPAddr, ::Integer)
show(::IO, ::Address)
DaskDistributedDispatcher.connect(::Address)
DaskDistributedDispatcher.pack(::Base.AbstractIOBuffer{Vector{UInt8}}, ::Address)
DaskDistributedDispatcher.parse_address(::String)
```
