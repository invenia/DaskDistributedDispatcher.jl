module DaskDistributedDispatcher

export Client,
    submit,
    result,
    cancel,
    gather,
    shutdown,
    default_client

export Worker,
    address

using DataStructures
using DeferredFutures
using Dispatcher
using Memento
using MsgPack
using URIParser

const level = "debug"  # other options are "debug", "info", "notice", "warn", etc.

const logger = Memento.config(level; fmt="[{level} | {name}]: {msg}")

include("comm.jl")
include("client.jl")
include("utils_address.jl")
include("utils_comm.jl")
include("worker.jl")

end # module
