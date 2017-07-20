module DaskDistributedDispatcher

export Client,
    submit,
    result,
    cancel,
    gather,
    replicate,
    shutdown,
    default_client,
    get_key

export Worker

export Server,
    start_listening

export Address

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
include("worker.jl")

end # module
