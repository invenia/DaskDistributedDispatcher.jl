# Communication

All communication between the julia client and workers with the scheduler is sent using the MsgPack protocol as [`specified by the dask-scheduler`](https://distributed.readthedocs.io/en/latest/protocol.html). Workers also use this to commmunicate between themselves and gather dependencies. TCP connections are used for all communication. Julia functions, arguments, and keyword arguments are serialized before being sent. [`Worker`](@ref)s and [`Client`](@ref)s should all belong to the same julia cluster or will not be able to communicate properly.

## API 
(For Internal Use)

```@docs
DaskDistributedDispatcher.send_recv{T}(::TCPSocket, ::Dict{String, T})
DaskDistributedDispatcher.send_msg(::TCPSocket, ::Union{String, Array, Dict})
DaskDistributedDispatcher.recv_msg(::TCPSocket)
DaskDistributedDispatcher.close_comm(::TCPSocket)
DaskDistributedDispatcher.read_msg(::Any)
DaskDistributedDispatcher.to_serialize(::Any)
DaskDistributedDispatcher.to_deserialize(::Vector{UInt8})
DaskDistributedDispatcher.pack_data(::Any, ::Dict{String, Any})
DaskDistributedDispatcher.pack_object(::Any, ::Dict{String, Any})
DaskDistributedDispatcher.unpack_data(::Any)
DaskDistributedDispatcher.unpack_object(::Any)
```

## Server
(For Internal Use)

```@docs
DaskDistributedDispatcher.Server
DaskDistributedDispatcher.start_listening(::DaskDistributedDispatcher.Server)
DaskDistributedDispatcher.handle_comm(::DaskDistributedDispatcher.Server, ::TCPSocket)
```

## Rpc
(For Internal Use)

```@docs
DaskDistributedDispatcher.Rpc
DaskDistributedDispatcher.Rpc(::Address)
DaskDistributedDispatcher.send_recv{T}(::DaskDistributedDispatcher.Rpc, ::Dict{String, T})
DaskDistributedDispatcher.start_comm(::DaskDistributedDispatcher.Rpc)
DaskDistributedDispatcher.get_comm(::DaskDistributedDispatcher.Rpc)
DaskDistributedDispatcher.close(::DaskDistributedDispatcher.Rpc)
```

## ConnectionPool 
(For Internal Use)

```@docs
DaskDistributedDispatcher.ConnectionPool
DaskDistributedDispatcher.ConnectionPool(::Integer)
DaskDistributedDispatcher.send_recv{T}(::DaskDistributedDispatcher.ConnectionPool, ::DaskDistributedDispatcher.Address, ::Dict{String, T})
DaskDistributedDispatcher.get_comm(::DaskDistributedDispatcher.ConnectionPool, ::DaskDistributedDispatcher.Address)
DaskDistributedDispatcher.reuse(::DaskDistributedDispatcher.ConnectionPool, ::DaskDistributedDispatcher.Address, ::TCPSocket)
DaskDistributedDispatcher.collect_comms(::DaskDistributedDispatcher.ConnectionPool)
DaskDistributedDispatcher.close(::DaskDistributedDispatcher.ConnectionPool)
```

## BatchedSend 
(For Internal Use)

```@docs
DaskDistributedDispatcher.BatchedSend
DaskDistributedDispatcher.BatchedSend(::TCPSocket)
DaskDistributedDispatcher.background_send(::DaskDistributedDispatcher.BatchedSend)
DaskDistributedDispatcher.send_msg{T}(::DaskDistributedDispatcher.BatchedSend, ::Dict{String, T})
DaskDistributedDispatcher.close(::DaskDistributedDispatcher.BatchedSend)
```
