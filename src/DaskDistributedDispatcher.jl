module DaskDistributedDispatcher

export DaskExecutor,
    dispatch!,
    reset!

export Client,
    submit,
    cancel,
    gather,
    replicate,
    shutdown,
    get_key

export Worker

export Address

using AutoHashEquals
using Compat
using DataStructures
using DeferredFutures
using Dispatcher
using Memento
using MsgPack

import Base.==

const logger = get_logger(current_module())

include("address.jl")
include("utils_comm.jl")
include("comm.jl")
include("client.jl")
include("executor.jl")
include("worker.jl")

end # module
