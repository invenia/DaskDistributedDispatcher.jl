module DaskDistributedDispatcher

export Client,
    submit,
    result,
    cancel,
    gather,
    shutdown,
    default_client,
    get_key

export Worker

export Server,
    address,
    start_listening

export Address

using Compat
using DataStructures
using DeferredFutures
using Dispatcher
using Memento
using MsgPack

import Base.==
const level = "debug"  # other options are "debug", "info", "notice", "warn", etc.

const logger = Memento.config(level; fmt="[{level} | {name}]: {msg}")

include("address.jl")
include("comm.jl")
include("client.jl")
include("utils_comm.jl")
include("worker.jl")

end # module
