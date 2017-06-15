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
    "text": "Dispatcher.jl builds the graph of julia computations and submits jobs to the  dask.distributed scheduler, which then determines when and where to schedule them. Thus, the computations can be scheduled and executed with a greater guarantee of effiency.Pages = [\"pages/manual.md\", \"pages/api.md\"]"
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
    "text": "Using the dask.distributed sheduler with Dispatcher.jl is designed to guarantee a stronger degree of effiency and allow for fluctuating worker resources. This also avoids pre-allocating tasks.The dask.distributed scheduler can be used in a julia workflow enviroment since it communicates entirely using msgpack and long bytestrings, making it language agnostic (no information that passes in or out of it is Python-specific)."
},

{
    "location": "pages/manual.html#Design-1",
    "page": "Manual",
    "title": "Design",
    "category": "section",
    "text": "The key components of this system are: <ul> <li>the dask-scheduler process that schedules computations and manages state</li> <li>a julia client used by Dispatcher.jl that submits work to the scheduler</li> <li>julia workers that accept instructions from the scheduler, fetch dependencies, execute compuations, store data, and communicates state to the scheduler</li> </ul>"
},

{
    "location": "pages/manual.html#Usage-1",
    "page": "Manual",
    "title": "Usage",
    "category": "section",
    "text": "To use this package you need to install Dask.Distributed and start a dask-scheduler process:$ dask-scheduler\nScheduler started at 127.0.0.1:8786Then start a julia process and startup a cluster of julia client/workers providing them the scheduler's address:using DaskDistributedDispatcher\naddprocs()\n@everywhere using DaskDistributedDispatcher\nclient = Client(\"127.0.0.1:8786\")\n@spawn worker = Worker(\"127.0.0.1:8786\")\n@spawn worker = Worker(\"127.0.0.1:8786\")Then you can submit Dispatcher Ops which represent units of compuation that can be run (a function call on some arguments) to the client which will relay it to the dask-scheduler and then be executed on a worker.op = Dispatcher.Op(Int, 2.0)\nsubmit(client, op)\nresult = result(client, op)"
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
    "text": "submit(client::Client, op::Dispatcher.Op)\n\nSubmit the Op computation unit to the dask-scheduler for computation.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.result-Tuple{DaskDistributedDispatcher.Client,Dispatcher.Op}",
    "page": "API",
    "title": "DaskDistributedDispatcher.result",
    "category": "Method",
    "text": "result(client::Client, op::Dispatcher.Op)\n\nGather the result of the Op computation unit. Requires there to be at least one worker available to the scheduler or hangs indefinetely.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.cancel-Tuple{DaskDistributedDispatcher.Client,Array}",
    "page": "API",
    "title": "DaskDistributedDispatcher.cancel",
    "category": "Method",
    "text": "cancel(client::Client, futures::Array)\n\nCancel all Ops in futures. This stops future tasks from being scheduled if they have not yet run and deletes them if they have already run. After calling, this result and all dependent results will no longer be accessible.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.gather-Tuple{DaskDistributedDispatcher.Client,Array}",
    "page": "API",
    "title": "DaskDistributedDispatcher.gather",
    "category": "Method",
    "text": "gather(client::Client, futures::Array)\n\nGather the results of all Ops in futures. Requires there to be at least one worker available to the scheduler or hangs indefinetely.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.shutdown-Tuple{DaskDistributedDispatcher.Client}",
    "page": "API",
    "title": "DaskDistributedDispatcher.shutdown",
    "category": "Method",
    "text": "shutdown(client::Client)\n\nTells the dask-scheduler to shutdown. This cancels all currently running tasks, clears the state of the scheduler, and shuts down all workers and scheduler. You only need to call this if you want to take down the distributed cluster.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.default_client-Tuple{}",
    "page": "API",
    "title": "DaskDistributedDispatcher.default_client",
    "category": "Method",
    "text": "default_client()\n\nReturn the default global client if a client has been registered with the dask-scheduler.\n\n\n\n"
},

{
    "location": "pages/api.html#Client-1",
    "page": "API",
    "title": "Client",
    "category": "section",
    "text": "Client\nClient(::String)\nsubmit(::Client, ::Dispatcher.Op)\nresult(::Client, ::Dispatcher.Op)\ncancel(::Client, ::Array)\ngather(::Client, ::Array)\nshutdown(::Client)\ndefault_client()"
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
    "location": "pages/api.html#Worker-1",
    "page": "API",
    "title": "Worker",
    "category": "section",
    "text": "Worker\nWorker(::String)"
},

]}
