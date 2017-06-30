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
    "text": "Dispatcher.jl builds the graph of julia computations and submits jobs to the  dask.distributed scheduler, which then determines when and where to schedule them. Thus, the computations can be scheduled and executed with a greater guarantee of effiency."
},

{
    "location": "index.html#Frequently-Asked-Questions-1",
    "page": "Home",
    "title": "Frequently Asked Questions",
    "category": "section",
    "text": "Isn't dask.distributed written in python?The dask.distributed scheduler can be used in a julia workflow environment since it is language agnostic (no information that passes in or out of it is Python-specific) but instead it communicates entirely using msgpack and long bytestrings."
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
    "text": "The key components of this system are:the dask-scheduler process that schedules computations and manages state\na julia client used by Dispatcher.jl that submits work to the scheduler\njulia workers that accept instructions from the scheduler, fetch dependencies, execute compuations, store data, and communicate state to the scheduler"
},

{
    "location": "pages/manual.html#Setup-1",
    "page": "Manual",
    "title": "Setup",
    "category": "section",
    "text": "To use this package you need to also install Dask.Distributed."
},

{
    "location": "pages/manual.html#Usage-1",
    "page": "Manual",
    "title": "Usage",
    "category": "section",
    "text": "First start a dask-scheduler process:$ dask-scheduler\nScheduler started at 127.0.0.1:8786Then, start a julia process and set up a cluster of julia client/workers providing them the scheduler's address:using DaskDistributedDispatcher\naddprocs()\n@everywhere using DaskDistributedDispatcher\nclient = Client(\"127.0.0.1:8786\")\n@spawn worker = Worker(\"127.0.0.1:8786\")\n@spawn worker = Worker(\"127.0.0.1:8786\")Then, you can submit Dispatcher Ops units of computation that can be run to the client which will relay it to the dask-scheduler to be scheduled and executed on a worker.op = Dispatcher.Op(Int, 2.0)\nsubmit(client, op)\nresult = result(client, op)"
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
