var documenterSearchIndex = {"docs": [

{
    "location": "index.html#",
    "page": "Home",
    "title": "Home",
    "category": "page",
    "text": ""
},

{
    "location": "index.html#DaskDistributedDispatcher.jl-1",
    "page": "Home",
    "title": "DaskDistributedDispatcher.jl",
    "category": "section",
    "text": "CurrentModule = DaskDistributedDispatcherDaskDistributedDispatcher integrates Dispatcher.jl with the python dask.distributed scheduler service."
},

{
    "location": "index.html#Overview-1",
    "page": "Home",
    "title": "Overview",
    "category": "section",
    "text": "Dispatcher.jl builds the graph of julia computations and submits jobs via the julia client to the  dask.distributed scheduler, which is in charge of determining when and where to schedule jobs on the julia workers. Thus, the computations are scheduled and executed efficiently."
},

{
    "location": "index.html#Frequently-Asked-Questions-1",
    "page": "Home",
    "title": "Frequently Asked Questions",
    "category": "section",
    "text": "How can the python dask.distributed scheduler be used for julia computations?The dask.distributed scheduler can be used in a julia workflow environment since it is language agnostic (no information that passes in or out of it is Python-specific). Instead the scheduler communicates with the workers/clients entirely using msgpack and long bytestrings."
},

{
    "location": "index.html#Documentation-Contents-1",
    "page": "Home",
    "title": "Documentation Contents",
    "category": "section",
    "text": "Pages = [\"pages/manual.md\", \"pages/api.md\"]"
},

{
    "location": "pages/manual.html#",
    "page": "Manual",
    "title": "Manual",
    "category": "page",
    "text": ""
},

{
    "location": "pages/manual.html#Manual-1",
    "page": "Manual",
    "title": "Manual",
    "category": "section",
    "text": ""
},

{
    "location": "pages/manual.html#Motivation-1",
    "page": "Manual",
    "title": "Motivation",
    "category": "section",
    "text": "The primary reason for integrating the dask.distributed sheduler with Dispatcher.jl is to be able to guarantee a stronger degree of effiency for computations run on Dispatcher and to allow for fluctuating worker resources."
},

{
    "location": "pages/manual.html#Design-1",
    "page": "Manual",
    "title": "Design",
    "category": "section",
    "text": "The key components of this system are:the dask-scheduler process that schedules computations and manages state\na julia client used by Dispatcher.jl that submits work to the scheduler\njulia workers that accept instructions from the scheduler, fetch dependencies, execute compuations, store data, and communicate state to the schedulerIn order to avoid redundant computations, the client will reuse previously computed results for identical operations."
},

{
    "location": "pages/manual.html#Setup-1",
    "page": "Manual",
    "title": "Setup",
    "category": "section",
    "text": "To use this package you also need to install Dask.Distributed."
},

{
    "location": "pages/manual.html#Usage-1",
    "page": "Manual",
    "title": "Usage",
    "category": "section",
    "text": "First, start a dask-scheduler process:$ dask-scheduler\nScheduler started at 127.0.0.1:8786Then, start a julia session and set up a cluster of julia client/workers, providing them the scheduler's address:using DaskDistributedDispatcher\nclient = Client(\"127.0.0.1:8786\")\n\naddprocs()\n@everywhere using DaskDistributedDispatcher\n\n@spawn worker = Worker(\"127.0.0.1:8786\")\n@spawn worker = Worker(\"127.0.0.1:8786\")You can then submit Dispatcher Ops units of computation that can be run to the client (which will relay it to the dask-scheduler to be scheduled and executed on a worker):op = Dispatcher.Op(Int, 2.0)\nsubmit(client, op)\nresult = result(client, op)Alternatively, you can get the results directly from the Op:result = fetch(op)If needed, you can specify which worker to run the computations on:using DaskDistributedDispatcher\nclient = Client(\"127.0.0.1:8786\")\n\npnums = addprocs(1)\n@everywhere using DaskDistributedDispatcher\n\nworker_address = @fetchfrom pnums[1] begin\n    worker = Worker(\"127.0.0.1:8786\")\n    return address(worker)\nend\n\nop = Dispatcher.Op(Int, 1.0)\nsubmit(client, op, workers=[worker_address])\nresult = result(client, op)Currently, if the Op submitted to the client results in an error, the result of the Op will then be a string representation of the error that occurred on the worker. This behaviour may change in the future.op = Dispatcher.Op(Int, 2.1)\nsubmit(client, op)\nresult = result(client, op) == \"InexactError\""
},

{
    "location": "pages/api.html#",
    "page": "API",
    "title": "API",
    "category": "page",
    "text": ""
},

{
    "location": "pages/api.html#API-1",
    "page": "API",
    "title": "API",
    "category": "section",
    "text": ""
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.Client",
    "page": "API",
    "title": "DaskDistributedDispatcher.Client",
    "category": "Type",
    "text": "Client\n\nA Client represents a client that the user can interact with to submit computations to the scheduler and gather results.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.Client-Tuple{String}",
    "page": "API",
    "title": "DaskDistributedDispatcher.Client",
    "category": "Method",
    "text": "Client(scheduler_address::String) -> Client\n\nConstruct a Client which can then be used to submit computations or gather results from the dask-scheduler process.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.submit-Tuple{DaskDistributedDispatcher.Client,Dispatcher.Op}",
    "page": "API",
    "title": "DaskDistributedDispatcher.submit",
    "category": "Method",
    "text": "submit(client::Client, op::Dispatcher.Op; workers::Array=[])\n\nSubmit the Op computation unit to the dask-scheduler for computation.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.result-Tuple{DaskDistributedDispatcher.Client,Dispatcher.Op}",
    "page": "API",
    "title": "DaskDistributedDispatcher.result",
    "category": "Method",
    "text": "result(client::Client, op::Dispatcher.Op) -> Any\n\nGather the result of the Op computation unit. Requires there to be at least one worker available to the scheduler or hangs indefinetely.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.cancel-Tuple{DaskDistributedDispatcher.Client,Array{Dispatcher.Op,N}}",
    "page": "API",
    "title": "DaskDistributedDispatcher.cancel",
    "category": "Method",
    "text": "cancel(client::Client, ops::Array{Dispatcher.Op})\n\nCancel all Ops in ops. This stops future tasks from being scheduled if they have not yet run and deletes them if they have already run. After calling, this result and all dependent results will no longer be accessible.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.gather-Tuple{DaskDistributedDispatcher.Client,Array{Dispatcher.Op,N}}",
    "page": "API",
    "title": "DaskDistributedDispatcher.gather",
    "category": "Method",
    "text": "gather(client::Client, ops::Array{Dispatcher.Op})\n\nGather the results of all ops. Requires there to be at least one worker available to the scheduler or hangs indefinetely waiting for the results.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.shutdown-Tuple{DaskDistributedDispatcher.Client}",
    "page": "API",
    "title": "DaskDistributedDispatcher.shutdown",
    "category": "Method",
    "text": "shutdown(client::Client)\n\nTell the dask-scheduler to close all workers and that this client is shutting down.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.default_client-Tuple{}",
    "page": "API",
    "title": "DaskDistributedDispatcher.default_client",
    "category": "Method",
    "text": "default_client()\n\nReturn the default global client if a client has been registered with the dask-scheduler.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.get_key-Tuple{Dispatcher.Op}",
    "page": "API",
    "title": "DaskDistributedDispatcher.get_key",
    "category": "Method",
    "text": "get_key(op::Dispatcher.Op)\n\nCalculate an identifying key for op. Keys are re-used for identical ops to avoid unnecessary computations.\n\n\n\n"
},

{
    "location": "pages/api.html#Client-1",
    "page": "API",
    "title": "Client",
    "category": "section",
    "text": "Client\nClient(::String)\nsubmit(::Client, ::Dispatcher.Op; ::Array)\nresult(::Client, ::Dispatcher.Op)\ncancel(::Client, ::Array{Dispatcher.Op})\ngather(::Client, ::Array{Dispatcher.Op})\nshutdown(::Client)\ndefault_client()\nget_key(::Dispatcher.Op)"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.Worker",
    "page": "API",
    "title": "DaskDistributedDispatcher.Worker",
    "category": "Type",
    "text": "Worker\n\nA Worker represents a worker endpoint in the distributed cluster that accepts instructions from the scheduler, fetches dependencies, executes compuations, stores data, and communicates state to the scheduler.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.Worker-Tuple{String}",
    "page": "API",
    "title": "DaskDistributedDispatcher.Worker",
    "category": "Method",
    "text": "Worker(scheduler_address::String)\n\nCreates a Worker that listens on a random port for incoming messages.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.address-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "API",
    "title": "DaskDistributedDispatcher.address",
    "category": "Method",
    "text": "address(worker::Worker)\n\nReturns this Workers's address formatted as an URI.\n\n\n\n"
},

{
    "location": "pages/api.html#Base.show-Tuple{IO,DaskDistributedDispatcher.Worker}",
    "page": "API",
    "title": "Base.show",
    "category": "Method",
    "text": "show(io::IO, worker::Worker)\n\nPrints a representation of the worker and it's state.\n\n\n\n"
},

{
    "location": "pages/api.html#Worker-1",
    "page": "API",
    "title": "Worker",
    "category": "section",
    "text": "Worker\nWorker(::String)\naddress(::Worker)\nshow(::IO, ::Worker)"
},

]}
