module DaskDistributedDispatcher

export Client,
    Worker,
    submit,
    shutdown,
    result,
    default_client,
    KeyedFuture,
    FutureState

using Dispatcher
using MsgPack
using URIParser
using Memento

const level = "debug"  # other options are "debug", "info", "notice", "warn", etc.

const logger = Memento.config(level; fmt="[{level} | {name}]: {msg}")

include("comm.jl")
include("client.jl")
include("addressing.jl")
include("utils_comm.jl")
include("worker.jl")

end # module
