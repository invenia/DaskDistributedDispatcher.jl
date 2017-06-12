module DaskDistributedDispatcher

export Client,
    Worker,
    submit,
    shutdown,
    KeyedFuture,
    result,
    FutureState

using Memento
using DeferredFutures
using Dispatcher
using MsgPack
using URIParser
using Etcd

const LOG_LEVEL = "debug"  # other options are "debug", "info", "notice", "warn", etc.

const logger = Memento.config(LOG_LEVEL; fmt="[{level} | {name}]: {msg}")

include("comm.jl")
include("client.jl")
include("keyedfuture.jl")
include("addressing.jl")
include("utils_comm.jl")
include("worker.jl")

end # module
