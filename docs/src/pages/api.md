# API

## DaskExecutor

```@docs
DaskExecutor
DaskExecutor(::String)
reset!(::DaskExecutor)
Dispatcher.dispatch!(::DaskExecutor, ::DispatchGraph)
DaskDistributedDispatcher.retries(::DaskExecutor)
DaskDistributedDispatcher.retry_on(::DaskExecutor)
DaskDistributedDispatcher.dispatch!(::DaskExecutor, ::Dispatcher.DispatchNode)
```

## Client

```@docs
Client
Client(::String)
submit(::Client, ::Dispatcher.DispatchNode; ::Array{Address,1})
cancel{T<:Dispatcher.DispatchNode}(::Client, ::Array{T, 1})
gather{T<:Dispatcher.DispatchNode}(::Client, ::Array{T, 1})
replicate{T<:Dispatcher.DispatchNode}(::Client; ::Array{T, 1})
shutdown(::Client)
get_key{T<:Dispatcher.DispatchNode}(node::T)
DaskDistributedDispatcher.ensure_connected(::Client)
DaskDistributedDispatcher.send_to_scheduler(::Client, ::Dict{String, DaskDistributedDispatcher.Message})
DaskDistributedDispatcher.serialize_deps{T<:Dispatcher.DispatchNode}(::Client, ::Array{T, 1}, ::Array{Array{UInt8, 1}}, ::Dict{Array{UInt8, 1}, Dict{String, Array{UInt8, 1}}}, ::Dict{Array{UInt8, 1}, Array{Array{UInt8, 1}}})
DaskDistributedDispatcher.serialize_node(::Client, ::Dispatcher.DispatchNode)
DaskDistributedDispatcher.serialize_task{T<:Dispatcher.DispatchNode}(::Client, node::T, ::Array{T, 1})
```

## Address

```@docs
Address
Address(::String)
Address(::IPAddr, ::Integer)
show(::IO, ::Address)
DaskDistributedDispatcher.connect(::Address)
DaskDistributedDispatcher.pack(::Base.AbstractIOBuffer{Array{UInt8,1}}, ::Address)
DaskDistributedDispatcher.parse_address(::String)
```
