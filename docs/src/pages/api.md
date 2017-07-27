# API

## DaskExecutor

```@docs
DaskExecutor
DaskExecutor(::String)
Dispatcher.dispatch!(::DaskExecutor, ::Dispatcher.DispatchContext)
reset!(::DaskExecutor)
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
DaskDistributedDispatcher.send_to_scheduler(::Client, ::Dict)
DaskDistributedDispatcher.serialize_deps(::Client, ::Array, ::Array, ::Dict, ::Dict)
DaskDistributedDispatcher.serialize_node(::Client, ::Dispatcher.DispatchNode)
DaskDistributedDispatcher.serialize_task{T<:Dispatcher.DispatchNode}(::Client, node::T, ::Array)
```

## Address

```@docs
Address
Address(::String)
Address(::Union{IPAddr, String}, ::Integer)
show(::IO, ::Address)
DaskDistributedDispatcher.connect(::Address)
DaskDistributedDispatcher.pack(::Base.AbstractIOBuffer{Array{UInt8,1}}, ::Address)
DaskDistributedDispatcher.parse_address(::String)
```
