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
    "text": "Dispatcher.jl builds the graph of julia computations and submits jobs via the julia client to the  dask.distributed scheduler, which is in charge of determining when and where to schedule jobs on the julia workers. Thus, the computations can be scheduled and executed efficiently."
},

{
    "location": "index.html#Frequently-Asked-Questions-1",
    "page": "Home",
    "title": "Frequently Asked Questions",
    "category": "section",
    "text": "How can the python dask.distributed scheduler be used for julia computations?The dask.distributed scheduler can be used in a julia workflow environment since it is language agnostic (no information that passes in or out of it is Python-specific). Instead the scheduler communicates with the workers/clients entirely using msgpack and long bytestrings. More information on the protocol used is here."
},

{
    "location": "index.html#Documentation-Contents-1",
    "page": "Home",
    "title": "Documentation Contents",
    "category": "section",
    "text": "Pages = [\"pages/manual.md\", \"pages/api.md\", \"pages/workers.md\", \"pages/communication.md\"]"
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
    "text": "The primary reason for integrating the dask.distributed scheduler with Dispatcher.jl is to be able to guarantee a stronger degree of effiency for computations run and to allow for fluctuating worker resources. (Note that removing workers from the worker pool may cause errors when fetching results. Only remove workers once you no longer need access to their information.)Using the dask-scheduler and executing compuations in a distributed manner can add overhead for simple tasks. Consider using an AsyncExecuter or ParallelExecuter if possible. The advantage that using the dask-scheduler has is that it schedules computations in a manner that is short-term-efficient and long-term-fair."
},

{
    "location": "pages/manual.html#Design-1",
    "page": "Manual",
    "title": "Design",
    "category": "section",
    "text": "The key components of this system are:the dask-scheduler process that schedules computations and manages state\na julia Client or DaskExecutor that submits work to the scheduler\njulia Workers that accept instructions from the scheduler, fetch dependencies, execute compuations, store data, and communicate state to the scheduler"
},

{
    "location": "pages/manual.html#Prerequisites-1",
    "page": "Manual",
    "title": "Prerequisites",
    "category": "section",
    "text": "Python 2.7 or 3.5, conda or pip, and the python package dask.distributed need to be installed (instructions here) before using this package. The minimum required version of the dask distributed package is >= v1.18.1."
},

{
    "location": "pages/manual.html#Setup-1",
    "page": "Manual",
    "title": "Setup",
    "category": "section",
    "text": "First, start a dask-scheduler process in a terminal:$ dask-scheduler\nScheduler started at 127.0.0.1:8786Then, in a julia session, set up a cluster of julia processes and initialize the workers by providing them with the dask-scheduler's tcp address:using DaskDistributedDispatcher\n\naddprocs(3)\n@everywhere using DaskDistributedDispatcher\n\nfor i in 1:3\n    @spawn Worker(\"127.0.0.1:8786\")\nend"
},

{
    "location": "pages/manual.html#Usage-1",
    "page": "Manual",
    "title": "Usage",
    "category": "section",
    "text": "Submit DispatchNodes units of computation that can be run to the DaskExecutor (which will relay them to the dask-scheduler to be scheduled and executed on a Worker):using Dispatcher\nusing ResultTypes\n\ndata = [1, 2, 3]\n\na = @op 1 + 2\nx = @op a + 3\ny = @op a + 1\n\nresult = @op x * y\n\nexecutor = DaskExecutor(\"127.0.0.1:8786\")\n(run_result,) = run!(executor, [result])\n\nrun_future = unwrap(run_result)\n@assert fetch(run_future) == 24Note: There must be at least one Worker running or else run! will hang indefinetely. Also, if the dask-scheduler is running on the same machine as the DaskExecutor and on its default port (8786), the address can be ommitted when initializing Workers and DaskExecutors.@spawn Worker()\n\nexecutor = DaskExecutor()See DaskExecutor and Dispatcher.jl for more usage information."
},

{
    "location": "pages/manual.html#Additional-notes-1",
    "page": "Manual",
    "title": "Additional notes",
    "category": "section",
    "text": "Using a Channel and/or Future in computations submitted to the DaskExecutor is not supported. Instead use a DeferredChannel or DeferredFuture. It is possible to bypass the DaskExecutor by accessing its client variable for more advanced workflows such as cancelling previously submitted computations or asking the scheduler to replicate data across all workers. See Client for more information.When done your computations, to get the dask-scheduler to reset and delete all previously computed values without restarting the Workers and the dask-scheduler call:reset!(executor)"
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
    "location": "pages/api.html#DaskDistributedDispatcher.DaskExecutor",
    "page": "API",
    "title": "DaskDistributedDispatcher.DaskExecutor",
    "category": "Type",
    "text": "DaskExecutor is an Executor  which executes julia computations as scheduled by the python dask-scheduler. It can run computations both asynchronously or in parallel (if Workers are started on a julia cluster instead).\n\nDaskExecutor's dispatch!(::DaskExecutor, ::Dispatcher.DispatchNode) method will complete as long as there are no cycles in the computation graph, the dask-scheduler remains online, and there is at least one Worker that is listening to the dask-scheduler.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.DaskExecutor-Tuple{String}",
    "page": "API",
    "title": "DaskDistributedDispatcher.DaskExecutor",
    "category": "Method",
    "text": "DaskExecutor(scheduler_address::String=\"127.0.0.1:8786\")\n\nReturn a new DaskExecutor. The scheduler_address only needs to be included if the dask-scheduler  is running on a different machine or not on it's default port (8786).\n\nNOTE: A dask-scheduler must be running at all times or the DaskExecutor execution will fail. If the scheduler is taken offline during execution for some reason, any remaining operations will fail to complete. Start a dask-scheduler from a terminal by typing dask-scheduler:\n\n$ dask-scheduler\nStart scheduler at 192.168.0.1:8786\n\nPrerequisites\n\npython 2.7 or 3.5\nthe python dask.distributed package (instructions for install here)\n\nNote that using the dask-scheduler and executing compuations in a distributed manner can add overhead for simple tasks. Consider using an AsyncExecuter or ParallelExecuter if possible. The advantage that using the dask-scheduler has is that it schedules computations in a manner that is short-term-efficient and long-term-fair.\n\nUsage\n\nThe DaskExecutor can run both asynchronously with the Workers, or in parallel if Workers are spawned on separate julia processes in a cluster.\n\nNOTE: Users must startup at least one Worker by pointing it to the dask-scheduler's address or else run! will hang indefinetely.\n\nExamples\n\nRunning asynchronously:\n\n# Reminder: make sure the dask-scheduler is running\nusing DaskDistributedDispatcher\nusing Dispatcher\nusing ResultTypes\n\nWorker()\n\nexec = DaskExecutor()\n\na = Op(()->3)\nb = Op(()->4)\nc = Op(max, a, b)\n\nresults = run!(exec, DispatchGraph(c))\n\nfetch(unwrap(results[1]))  # 4\n\nRunning in parallel:\n\n# Reminder: make sure the dask-scheduler is running\nusing DaskDistributedDispatcher\nusing Dispatcher\nusing ResultTypes\n\naddprocs(3)\n@everywhere using DaskDistributedDispatcher\n\nfor i in 1:3\n    cond = @spawn Worker()\n    wait(cond)\nend\n\nexec = DaskExecutor()\n\na = Op(()->3)\nb = Op(()->4)\nc = Op(max, a, b)\n\nresults = run!(exec, DispatchGraph(c))\n\nfetch(unwrap(results[1]))  # 4\n\nTo delete all previously computed information from the workers:\n\nreset!(exec)\n\nAdvanced Workflows\n\nIt is possible to bypass the DaskExecutor and use the Client directly to submit compuations, cancel previously scheduled DispatchNodes, gather results, or replicate data across all workers. See Client for more details. It is recommened to start with a DaskExecutor and access its client field if needed later on.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.reset!-Tuple{DaskDistributedDispatcher.DaskExecutor}",
    "page": "API",
    "title": "DaskDistributedDispatcher.reset!",
    "category": "Method",
    "text": "reset!(exec::DaskExecutor)\n\nRestarts the executor's Client, which tells the scheduler to delete previously computed data since it is not needed anymore. The scheduler, in turn, signals this to the workers.\n\n\n\n"
},

{
    "location": "pages/api.html#Dispatcher.run_inner_node!-Tuple{DaskDistributedDispatcher.DaskExecutor,Dispatcher.DispatchNode,Int64}",
    "page": "API",
    "title": "Dispatcher.run_inner_node!",
    "category": "Method",
    "text": "run_inner_node!(exec::DaskExecutor, node::DispatchNode, id::Int)\n\nSubmit the DispatchNode at position id in the DispatchGraph for scheduling and execution. Any error thrown during the node's execution is caught and wrapped in a DependencyError.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.retries-Tuple{DaskDistributedDispatcher.DaskExecutor}",
    "page": "API",
    "title": "DaskDistributedDispatcher.retries",
    "category": "Method",
    "text": "retries(exec::DaskExecutor) -> Int\n\nReturn the number of retries per node.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.retry_on-Tuple{DaskDistributedDispatcher.DaskExecutor}",
    "page": "API",
    "title": "DaskDistributedDispatcher.retry_on",
    "category": "Method",
    "text": "retry_on(exec::DaskExecutor) -> Vector{Function}\n\nReturn the array of retry conditions.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.dispatch!-Tuple{DaskDistributedDispatcher.DaskExecutor,Dispatcher.DispatchNode}",
    "page": "API",
    "title": "DaskDistributedDispatcher.dispatch!",
    "category": "Method",
    "text": "dispatch!(exec::DaskExecutor, node::Dispatcher.DispatchNode) -> Future\n\ndispatch! takes the DaskExecutor and a DispatchNode to run and submits the DispatchNode to the Client for scheduling.\n\nThis is the defining method of DaskExecutor.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskExecutor-1",
    "page": "API",
    "title": "DaskExecutor",
    "category": "section",
    "text": "DaskExecutor\nDaskExecutor(::String)\nreset!(::DaskExecutor)\nDispatcher.run_inner_node!(::DaskExecutor, ::Dispatcher.DispatchNode, ::Int)\nDaskDistributedDispatcher.retries(::DaskExecutor)\nDaskDistributedDispatcher.retry_on(::DaskExecutor)\nDaskDistributedDispatcher.dispatch!(::DaskExecutor, ::Dispatcher.DispatchNode)"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.Client",
    "page": "API",
    "title": "DaskDistributedDispatcher.Client",
    "category": "Type",
    "text": "Client\n\nClient that can be interacted with to submit computations to the scheduler and gather results. Should only be used directly for advanced workflows. See DaskExecutor instead for normal usage.\n\nFields\n\nkeys::Set{String}: previously submitted keys\nid::String: this client's identifier\nstatus::String: status of this client\nscheduler_address::Address: the dask-distributed scheduler ip address and port info\nscheduler::Rpc: manager for discrete send/receive open connections to the scheduler\nscheduler_comm::Nullable{BatchedSend}: batched stream for communication with scheduler\npending_msg_buffer::Vector{Dict{String, Any}}: pending msgs to send to the scheduler\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.Client-Tuple{String}",
    "page": "API",
    "title": "DaskDistributedDispatcher.Client",
    "category": "Method",
    "text": "Client(scheduler_address::String) -> Client\n\nConstruct a Client which can then be used to submit computations or gather results from the dask-scheduler process.\n\nUsage\n\nusing DaskDistributedDispatcher\nusing Dispatcher\n\naddprocs(3)\n@everywhere using DaskDistributedDispatcher\n\nfor i in 1:3\n    @spawn Worker(\"127.0.0.1:8786\")\nend\n\nclient = Client(\"127.0.0.1:8786\")\n\nop = Op(Int, 2.0)\nsubmit(client, op)\nresult = fetch(op)\n\nPreviously submitted Ops can be cancelled by calling:\n\ncancel(client, [op])\n\n# Or if using the `DaskExecutor`\ncancel(executor.client, [op])\n\nIf needed, which worker(s) to run the computations on can be explicitly specified by returning the worker's address when starting a new worker:\n\nusing DaskDistributedDispatcher\nclient = Client(\"127.0.0.1:8786\")\n\npnums = addprocs(1)\n@everywhere using DaskDistributedDispatcher\n\nworker_address = @fetchfrom pnums[1] begin\n    worker = Worker(\"127.0.0.1:8786\")\n    return worker.address\nend\n\nop = Op(Int, 1.0)\nsubmit(client, op, workers=[worker_address])\nresult = result(client, op)\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.submit-Tuple{DaskDistributedDispatcher.Client,Dispatcher.DispatchNode}",
    "page": "API",
    "title": "DaskDistributedDispatcher.submit",
    "category": "Method",
    "text": "submit(client::Client, node::DispatchNode; workers::Vector{Address}=Address[])\n\nSubmit the node computation unit to the dask-scheduler for computation. Also submits all node's dependencies to the scheduler if they have not previously been submitted.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.cancel-Tuple{DaskDistributedDispatcher.Client,Array{T<:Dispatcher.DispatchNode,1}}",
    "page": "API",
    "title": "DaskDistributedDispatcher.cancel",
    "category": "Method",
    "text": "cancel{T<:DispatchNode}(client::Client, nodes::Vector{T})\n\nCancel all DispatchNodes in nodes. This stops future tasks from being scheduled if they have not yet run and deletes them if they have already run. After calling, this result and all dependent results will no longer be accessible.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.gather-Tuple{DaskDistributedDispatcher.Client,Array{T<:Dispatcher.DispatchNode,1}}",
    "page": "API",
    "title": "DaskDistributedDispatcher.gather",
    "category": "Method",
    "text": "gather{T<:DispatchNode}(client::Client, nodes::Vector{T}) -> Vector\n\nGather the results of all nodes. Requires there to be at least one worker available to the scheduler or hangs indefinetely waiting for the results.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.replicate-Tuple{DaskDistributedDispatcher.Client}",
    "page": "API",
    "title": "DaskDistributedDispatcher.replicate",
    "category": "Method",
    "text": "replicate{T<:DispatchNode}(client::Client; nodes::Vector{T}=DispatchNode[])\n\nCopy data onto many workers. Helps to broadcast frequently accessed data and improve resilience. By default replicates all nodes that have been submitted by this client unless they have been cancelled.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.shutdown-Tuple{DaskDistributedDispatcher.Client}",
    "page": "API",
    "title": "DaskDistributedDispatcher.shutdown",
    "category": "Method",
    "text": "shutdown(client::Client)\n\nTell the dask-scheduler that this client is shutting down. Does NOT terminate the scheduler itself nor the workers. This does not have to be called after a session but is useful to delete all the information submitted by the client from the scheduler and workers (such as between test runs). To reconnect to the scheduler after calling this function set up a new client.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.get_key-Tuple{T<:Dispatcher.DispatchNode}",
    "page": "API",
    "title": "DaskDistributedDispatcher.get_key",
    "category": "Method",
    "text": "get_key{T<:DispatchNode}(node::T)\n\nCalculate an identifying key for node. Keys are re-used for identical nodes to avoid unnecessary computations.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.ensure_connected-Tuple{DaskDistributedDispatcher.Client}",
    "page": "API",
    "title": "DaskDistributedDispatcher.ensure_connected",
    "category": "Method",
    "text": "ensure_connected(client::Client)\n\nEnsure the client is connected to the dask-scheduler. For internal use.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.send_to_scheduler-Tuple{DaskDistributedDispatcher.Client,Dict{String,Any}}",
    "page": "API",
    "title": "DaskDistributedDispatcher.send_to_scheduler",
    "category": "Method",
    "text": "send_to_scheduler(client::Client, msg::Dict{String, Any})\n\nSend msg to the dask-scheduler that the client is connected to. For internal use.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.serialize_deps-Tuple{DaskDistributedDispatcher.Client,Array{T<:Dispatcher.DispatchNode,1},Array{String,1},Dict{String,Dict{String,Array{UInt8,1}}},Dict{String,Array{String,1}}}",
    "page": "API",
    "title": "DaskDistributedDispatcher.serialize_deps",
    "category": "Method",
    "text": "serialize_deps{T<:DispatchNode}(args...) -> Tuple\n\nSerialize dependencies to send to the scheduler.\n\nArguments\n\nclient::Client\ndeps::Vector{T}: the node dependencies to be serialized\nkeys::Vector{String}: list of all keys that have already been serialized\ntasks::Dict{String, Dict{String, Vector{UInt8}}}: serialized tasks\ntasks_deps::Dict{String, Vector{String}}: dependencies for each task\n\nReturns\n\nTuple{Vector{String}, Dict{String, Dict{String, Vector{UInt8}}}, Dict{String, Vector{String}}}:   keys, serialized tasks, and task dependencies that will be sent to the scheduler\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.serialize_node-Tuple{DaskDistributedDispatcher.Client,Dispatcher.DispatchNode}",
    "page": "API",
    "title": "DaskDistributedDispatcher.serialize_node",
    "category": "Method",
    "text": "serialize_node(client::Client, node::DispatchNode) -> Tuple\n\nSerialize node into it's task and dependencies. For internal use.\n\nReturns\n\nTuple{Vector{DispatchNode}, Dict{String, Vector{UInt8}}, Vector{String}}: tuple of the   task dependencies nodes that are yet to be serialized, the serialized task, and the   keys of the serialized task's dependencies that will be sent to the scheduler\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.serialize_task-Tuple{DaskDistributedDispatcher.Client,T<:Dispatcher.DispatchNode,Array{T<:Dispatcher.DispatchNode,1}}",
    "page": "API",
    "title": "DaskDistributedDispatcher.serialize_task",
    "category": "Method",
    "text": "serialize_task{T<:DispatchNode}(client::Client, node::T, deps::Vector{T}) -> Dict\n\nSerialize node into its components. For internal use.\n\n\n\n"
},

{
    "location": "pages/api.html#Client-1",
    "page": "API",
    "title": "Client",
    "category": "section",
    "text": "Client\nClient(::String)\nsubmit(::Client, ::Dispatcher.DispatchNode)\ncancel{T<:Dispatcher.DispatchNode}(::Client, ::Vector{T})\ngather{T<:Dispatcher.DispatchNode}(::Client, ::Vector{T})\nreplicate{T<:Dispatcher.DispatchNode}(::Client)\nshutdown(::Client)\nget_key{T<:Dispatcher.DispatchNode}(node::T)\nDaskDistributedDispatcher.ensure_connected(::Client)\nDaskDistributedDispatcher.send_to_scheduler(::Client, ::Dict{String, Any})\nDaskDistributedDispatcher.serialize_deps{T<:Dispatcher.DispatchNode}(::Client, ::Vector{T}, ::Vector{String}, ::Dict{String, Dict{String, Vector{UInt8}}}, ::Dict{String, Vector{String}})\nDaskDistributedDispatcher.serialize_node(::Client, ::Dispatcher.DispatchNode)\nDaskDistributedDispatcher.serialize_task{T<:Dispatcher.DispatchNode}(::Client, node::T, ::Vector{T})"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.Address",
    "page": "API",
    "title": "DaskDistributedDispatcher.Address",
    "category": "Type",
    "text": "Address\n\nA representation of an endpoint that can be connected to. It is categorized by its scheme (tcp is currently the only protocol supported), host, and port.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.Address-Tuple{String}",
    "page": "API",
    "title": "DaskDistributedDispatcher.Address",
    "category": "Method",
    "text": "Address(address::String) -> Address\n\nParse address and returns the corresponding Address object.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.Address-Tuple{IPAddr,Integer}",
    "page": "API",
    "title": "DaskDistributedDispatcher.Address",
    "category": "Method",
    "text": "Address(host::IPAddr, port::Integer)) -> Address\n\nReturn the corresponding Address object to the components host and port. By default the tcp protocol is assumed.\n\n\n\n"
},

{
    "location": "pages/api.html#Base.show-Tuple{IO,DaskDistributedDispatcher.Address}",
    "page": "API",
    "title": "Base.show",
    "category": "Method",
    "text": "show(io::IO, address::Address)\n\nPrint a representation of the address to io. The format used to represent addresses is \"tcp://127.0.0.1:port\".\n\n\n\n"
},

{
    "location": "pages/api.html#Base.connect-Tuple{DaskDistributedDispatcher.Address}",
    "page": "API",
    "title": "Base.connect",
    "category": "Method",
    "text": "Base.connect(address::Address)\n\nOpen a tcp connection to address.\n\n\n\n"
},

{
    "location": "pages/api.html#MsgPack.pack-Tuple{Base.AbstractIOBuffer{Array{UInt8,1}},DaskDistributedDispatcher.Address}",
    "page": "API",
    "title": "MsgPack.pack",
    "category": "Method",
    "text": "MsgPack.pack(io::Base.AbstractIOBuffer{Vector{UInt8}}, address::Address)\n\nPack address as its string representation.\n\n\n\n"
},

{
    "location": "pages/api.html#DaskDistributedDispatcher.parse_address-Tuple{String}",
    "page": "API",
    "title": "DaskDistributedDispatcher.parse_address",
    "category": "Method",
    "text": "parse_address(address::String) -> (String, IPAddr, UInt16)\n\nParse an address into its scheme, host, and port components.\n\n\n\n"
},

{
    "location": "pages/api.html#Address-1",
    "page": "API",
    "title": "Address",
    "category": "section",
    "text": "Address\nAddress(::String)\nAddress(::IPAddr, ::Integer)\nshow(::IO, ::Address)\nDaskDistributedDispatcher.connect(::Address)\nDaskDistributedDispatcher.pack(::Base.AbstractIOBuffer{Vector{UInt8}}, ::Address)\nDaskDistributedDispatcher.parse_address(::String)"
},

{
    "location": "pages/workers.html#",
    "page": "Workers",
    "title": "Workers",
    "category": "page",
    "text": ""
},

{
    "location": "pages/workers.html#Workers-1",
    "page": "Workers",
    "title": "Workers",
    "category": "section",
    "text": "Julia workers were developed that integrate with the python dask-scheduler, and hence follow many of the same patterns that the python dask-workers do."
},

{
    "location": "pages/workers.html#Notable-Differences-1",
    "page": "Workers",
    "title": "Notable Differences",
    "category": "section",
    "text": "The julia workers don't execute computations in a thread pool but rather do so asynchronously. The recommended way to setup the workers is to use addprocs and spawn at least one Worker per julia process added in the cluster.\nCurrently the julia workers do not support specifying resources needed by computations or spilling excess data onto disk."
},

{
    "location": "pages/workers.html#Tasks-1",
    "page": "Workers",
    "title": "Tasks",
    "category": "section",
    "text": "Usually computations submitted to a worker go through task states in the following order:waiting -> ready -> executing -> memoryComputations that result in errors being thrown are caught and the error is saved in memory. Workers communicate between themselves to gather dependencies and with the dask-scheduler."
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.Worker",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.Worker",
    "category": "Type",
    "text": "Worker\n\nA Worker represents a worker endpoint in the distributed cluster. It accepts instructions from the scheduler, fetches dependencies, executes compuations, stores data, and communicates state to the scheduler.\n\nFields\n\nstatus::Symbol: status of this worker\naddress::Address:: ip address and port that this worker is listening on\nlistener::Base.TCPServer: tcp server that listens for incoming connections\nscheduler_address::Address: the dask-distributed scheduler ip address and port info\nbatched_stream::Nullable{BatchedSend}: batched stream for communication with scheduler\nscheduler::Rpc: manager for discrete send/receive open connections to the scheduler\nconnection_pool::ConnectionPool: manages connections to peers\nhandlers::Dict{String, Function}: handlers for operations requested by open connections\ncompute_stream_handlers::Dict{String, Function}: handlers for compute stream operations\ntransitions::Dict{Tuple, Function}: valid transitions that a task can make\ndata_needed::Deque{String}: keys whose data we still lack\nready::PriorityQueue{String, Tuple, Base.Order.ForwardOrdering}: keys ready to run\ndata::Dict{String, Any}: maps keys to the results of function calls (actual values)\ntasks::Dict{String, Tuple}: maps keys to the function, args, and kwargs of a task\ntask_state::Dict{String, Symbol}: maps keys tot heir state: (waiting, executing, memory)\npriorities::Dict{String, Tuple}: run time order priority of a key given by the scheduler\npriority_counter::Int: used to prioritize tasks by their order of arrival\ndep_transitions::Dict{Tuple, Function}: valid transitions that a dependency can make\ndep_state::Dict{String, Symbol}: maps dependencies with their state   (waiting, flight, memory)\ndependencies::Dict{String, Set}: maps a key to the data it needs to run\ndependents::Dict{String, Set}: maps a dependency to the keys that use it\nwaiting_for_data::Dict{String, Set}: maps a key to the data it needs that we don't have\npending_data_per_worker::DefaultDict{String, Deque}: data per worker that we want\nwho_has::Dict{String, Set}: maps keys to the workers believed to have their data\nhas_what::DefaultDict{String, Set{String}}: maps workers to the data they have\nin_flight_workers::Dict{String, Set}: workers from which we are getting data from\nmissing_dep_flight::Set{String}: missing dependencies\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.Worker-Tuple{String}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.Worker",
    "category": "Method",
    "text": "Worker(scheduler_address::String=\"127.0.0.1:8786\")\n\nCreate a Worker that listens on a random port between 1024 and 9000 for incoming messages. By default if the scheduler's address is not provided it assumes that the dask-scheduler is being run on the same machine and on the default port 8786.\n\nNOTE: Worker's must be started in the same julia cluster as the DaskExecutor (and it's Client).\n\nUsage\n\nWorker()  # The dask-scheduler is being run on the same machine on its default port 8786.\n\nor also\n\nWorker(\"$(getipaddr()):8786\") # Scheduler is running on the same machine\n\nIf running the dask-scheduler on a different machine or port:\n\nFirst start the dask-scheduler and inspect its startup logs:\n\n$ dask-scheduler\ndistributed.scheduler - INFO - -----------------------------------------------\ndistributed.scheduler - INFO -   Scheduler at:   tcp://127.0.0.1:8786\ndistributed.scheduler - INFO - etc.\ndistributed.scheduler - INFO - -----------------------------------------------\n\nThen start workers with it's printed address:\n\nWorker(\"tcp://127.0.0.1:8786\")\n\nNo further actions are needed directly on the Worker's themselves as they will communicate with the dask-scheduler independently. New Workers can be added/removed at any time during execution. There usually should be at least one Worker to run computations.\n\nCleanup\n\nTo explicitly shutdown a worker and delete it's information use:\n\nworker = Worker()\nshutdown([worker.address])\n\nIt is more effective to explicitly reset the DaskExecutor or shutdown a Client rather than a Worker because the dask-scheduler will automatically re-schedule the lost computations on other Workers if it thinks that a Client still needs the lost data.\n\nWorker's are lost if they were spawned on a julia process that exits or is removed via rmprocs from the julia cluster. It is cleaner but not necessary to explicity call shutdown if planning to remove a Worker.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.shutdown-Tuple{Array{DaskDistributedDispatcher.Address,1}}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.shutdown",
    "category": "Method",
    "text": "shutdown(workers::Vector{Address})\n\nConnect to and terminate all workers in workers.\n\n\n\n"
},

{
    "location": "pages/workers.html#Base.show-Tuple{IO,DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "Base.show",
    "category": "Method",
    "text": "show(io::IO, worker::Worker)\n\nPrint a representation of the worker and it's state.\n\n\n\n"
},

{
    "location": "pages/workers.html#API-1",
    "page": "Workers",
    "title": "API",
    "category": "section",
    "text": "Worker\nWorker(::String)\nshutdown(::Vector{Address})\nshow(::IO, ::Worker)"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.start-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.start",
    "category": "Method",
    "text": "start(worker::Worker)\n\nCoordinate a worker's startup.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.register-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.register",
    "category": "Method",
    "text": "register(worker::Worker)\n\nRegister a Worker with the dask-scheduler process.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.handle_comm-Tuple{DaskDistributedDispatcher.Worker,TCPSocket}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.handle_comm",
    "category": "Method",
    "text": "handle_comm(worker::Worker, comm::TCPSocket)\n\nListen for incoming messages on an established connection.\n\n\n\n"
},

{
    "location": "pages/workers.html#Base.close-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "Base.close",
    "category": "Method",
    "text": "Base.close(worker::Worker; report::String=\"true\")\n\nClose the worker and all the connections it has open.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.get_data-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.get_data",
    "category": "Method",
    "text": "get_data(worker::Worker; keys::Array=String[], who::String=\"\") -> Dict\n\nSend the results of keys back over the stream they were requested on.\n\nReturns\n\nDict{String, Vector{UInt8}}: dictionary mapping keys to their serialized data for   communication\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.gather-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.gather",
    "category": "Method",
    "text": "gather(worker::Worker; who_has::Dict=Dict{String, Vector{String}}())\n\nGather the results for various keys.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.update_data-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.update_data",
    "category": "Method",
    "text": "update_data(worker::Worker; data::Dict=Dict(), report::String=\"true\") -> Dict\n\nUpdate the worker data.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.delete_data-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.delete_data",
    "category": "Method",
    "text": "delete_data(worker::Worker; keys::Array=String[], report::String=\"true\")\n\nDelete the data associated with each key of keys in worker.data.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.terminate-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.terminate",
    "category": "Method",
    "text": "terminate(worker::Worker; report::String=\"true\")\n\nShutdown the worker and close all its connections.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.get_keys-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.get_keys",
    "category": "Method",
    "text": "get_keys(worker::Worker) -> Vector{String}\n\nGet a list of all the keys held by this worker for communication with scheduler and other workers.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.add_task-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.add_task",
    "category": "Method",
    "text": "add_task(worker::Worker; kwargs...)\n\nAdd a task to the worker's list of tasks to be computed.\n\nKeywords\n\nkey::String: The tasks's unique identifier. Throws an exception if blank.\npriority::Array: The priority of the task. Throws an exception if blank.\nwho_has::Dict: Map of dependent keys and the addresses of the workers that have them.\nfunc::Union{String, Vector{UInt8}}: The callable funtion for the task, serialized.\nargs::Union{String, Vector{UInt8}}}: The arguments for the task, serialized.\nkwargs::Union{String, Vector{UInt8}}: The keyword arguments for the task, serialized.\nfuture::Union{String, Vector{UInt8}}}: The tasks's serialized DeferredFuture.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.release_key-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.release_key",
    "category": "Method",
    "text": "release_key(worker::Worker; key::String=\"\", cause::String=\"\", reason::String=\"\")\n\nDelete a key and its data.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.release_dep-Tuple{DaskDistributedDispatcher.Worker,String}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.release_dep",
    "category": "Method",
    "text": "release_dep(worker::Worker, dep::String)\n\nDelete a dependency key and its data.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.ensure_computing-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.ensure_computing",
    "category": "Method",
    "text": "ensure_computing(worker::Worker)\n\nMake sure the worker is computing available tasks.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.execute-Tuple{DaskDistributedDispatcher.Worker,String}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.execute",
    "category": "Method",
    "text": "execute(worker::Worker, key::String)\n\nExecute the task identified by key.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.put_key_in_memory-Tuple{DaskDistributedDispatcher.Worker,String,Any}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.put_key_in_memory",
    "category": "Method",
    "text": "put_key_in_memory(worker::Worker, key::String, value; should_transition::Bool=true)\n\nStore the result (value) of the task identified by key.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.ensure_communicating-Tuple{DaskDistributedDispatcher.Worker}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.ensure_communicating",
    "category": "Method",
    "text": "ensure_communicating(worker::Worker)\n\nEnsure the worker is communicating with its peers to gather dependencies as needed.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.gather_dep-Tuple{DaskDistributedDispatcher.Worker,String,String,Set{String}}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.gather_dep",
    "category": "Method",
    "text": "gather_dep(worker::Worker, worker_addr::String, dep::String, deps::Set; cause::String=\"\")\n\nGather the dependency with identifier \"dep\" from worker_addr.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.handle_missing_dep-Tuple{DaskDistributedDispatcher.Worker,Set{String}}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.handle_missing_dep",
    "category": "Method",
    "text": "handle_missing_dep(worker::Worker, deps::Set{String})\n\nHandle a missing dependency that can't be found on any peers.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.update_who_has-Tuple{DaskDistributedDispatcher.Worker,Dict{String,Array{String,1}}}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.update_who_has",
    "category": "Method",
    "text": "update_who_has(worker::Worker, who_has::Dict{String, Vector{String}})\n\nEnsure who_has is up to date and accurate.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.select_keys_for_gather-Tuple{DaskDistributedDispatcher.Worker,String,String}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.select_keys_for_gather",
    "category": "Method",
    "text": "select_keys_for_gather(worker::Worker, worker_addr::String, dep::String)\n\nSelect which keys to gather from peer at worker_addr.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.gather_from_workers-Tuple{Dict{String,Array{String,1}},DaskDistributedDispatcher.ConnectionPool}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.gather_from_workers",
    "category": "Method",
    "text": "gather_from_workers(who_has::Dict, connection_pool::ConnectionPool) -> Tuple\n\nGather data directly from who_has peers.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.transition-Tuple{DaskDistributedDispatcher.Worker,String,Symbol}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.transition",
    "category": "Method",
    "text": "transition(worker::Worker, key::String, finish_state::Symbol; kwargs...)\n\nTransition task with identifier key to finish_state from its current state.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.transition_dep-Tuple{DaskDistributedDispatcher.Worker,String,Symbol}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.transition_dep",
    "category": "Method",
    "text": "transition_dep(worker::Worker, dep::String, finish_state::Symbol; kwargs...)\n\nTransition dependency task with identifier key to finish_state from its current state.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.send_task_state_to_scheduler-Tuple{DaskDistributedDispatcher.Worker,String}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.send_task_state_to_scheduler",
    "category": "Method",
    "text": "send_task_state_to_scheduler(worker::Worker, key::String)\n\nSend the state of task key to the scheduler.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.deserialize_task-Tuple{Array{UInt8,1},Array{UInt8,1},Array{UInt8,1},Array{UInt8,1}}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.deserialize_task",
    "category": "Method",
    "text": "deserialize_task(func, args, kwargs, future) -> Tuple\n\nDeserialize task inputs and regularize to func, args, kwargs.\n\nReturns\n\nTuple: The deserialized function, arguments, keyword arguments, and Deferredfuture for\n\nthe task.\n\n\n\n"
},

{
    "location": "pages/workers.html#DaskDistributedDispatcher.apply_function-Tuple{String,Union{DataType,Function},Tuple,Array{Any,1}}",
    "page": "Workers",
    "title": "DaskDistributedDispatcher.apply_function",
    "category": "Method",
    "text": "apply_function(key::String, func::Base.Callable, args::Any, kwargs::Any)\n\nRun a function and return collected information.\n\n\n\n"
},

{
    "location": "pages/workers.html#Internals-1",
    "page": "Workers",
    "title": "Internals",
    "category": "section",
    "text": "DaskDistributedDispatcher.start(::Worker)\nDaskDistributedDispatcher.register(::Worker)\nDaskDistributedDispatcher.handle_comm(::Worker, ::TCPSocket)\nDaskDistributedDispatcher.close(::Worker)\nDaskDistributedDispatcher.get_data(::Worker)\nDaskDistributedDispatcher.gather(::Worker)\nDaskDistributedDispatcher.update_data(::Worker)\nDaskDistributedDispatcher.delete_data(::Worker)\nDaskDistributedDispatcher.terminate(::Worker)\nDaskDistributedDispatcher.get_keys(::Worker)\nDaskDistributedDispatcher.add_task(::Worker)\nDaskDistributedDispatcher.release_key(::Worker)\nDaskDistributedDispatcher.release_dep(::Worker, ::String)\nDaskDistributedDispatcher.ensure_computing(::Worker)\nDaskDistributedDispatcher.execute(::Worker, ::String)\nDaskDistributedDispatcher.put_key_in_memory(::Worker, ::String, ::Any)\nDaskDistributedDispatcher.ensure_communicating(::Worker)\nDaskDistributedDispatcher.gather_dep(::Worker, ::String, ::String, ::Set{String})\nDaskDistributedDispatcher.handle_missing_dep(::Worker, ::Set{String})\nDaskDistributedDispatcher.update_who_has(::Worker, ::Dict{String, Vector{String}})\nDaskDistributedDispatcher.select_keys_for_gather(::Worker, ::String, ::String)\nDaskDistributedDispatcher.gather_from_workers(::Dict{String, Vector{String}}, ::DaskDistributedDispatcher.ConnectionPool)\nDaskDistributedDispatcher.transition(::Worker, ::String, ::Symbol)\nDaskDistributedDispatcher.transition_dep(::Worker, ::String, ::Symbol)\nDaskDistributedDispatcher.send_task_state_to_scheduler(::Worker, ::String)\nDaskDistributedDispatcher.deserialize_task(::Vector{UInt8}, ::Vector{UInt8}, ::Vector{UInt8}, ::Vector{UInt8})\nDaskDistributedDispatcher.apply_function(::String, ::Base.Callable, ::Tuple, ::Vector{Any})"
},

{
    "location": "pages/communication.html#",
    "page": "Communication",
    "title": "Communication",
    "category": "page",
    "text": ""
},

{
    "location": "pages/communication.html#Communication-1",
    "page": "Communication",
    "title": "Communication",
    "category": "section",
    "text": "All communication between the julia client and workers with the scheduler is sent using the MsgPack protocol as specified by the dask-scheduler. Workers also use this to commmunicate between themselves and gather dependencies. TCP connections are used for all communication. Julia functions, arguments, and keyword arguments are serialized before being sent. Workers and Clients should all belong to the same julia cluster or will not be able to communicate properly."
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.send_recv-Tuple{TCPSocket,Dict{String,T}}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.send_recv",
    "category": "Method",
    "text": "send_recv{T}(sock::TCPSocket, msg::Dict{String, T})\n\nSend a message and wait for the response.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.send_msg-Tuple{TCPSocket,Union{Array,Dict,String}}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.send_msg",
    "category": "Method",
    "text": "send_msg(sock::TCPSocket, msg::Union{String, Array, Dict})\n\nSend msg to sock serialized by MsgPack following the dask.distributed protocol.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.recv_msg-Tuple{TCPSocket}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.recv_msg",
    "category": "Method",
    "text": "recv_msg(sock::TCPSocket) -> Union{String, Array, Dict}\n\nRecieve msg from sock and deserialize it from msgpack encoded bytes to strings.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.close_comm-Tuple{TCPSocket}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.close_comm",
    "category": "Method",
    "text": "close_comm(comm::TCPSocket)\n\nTell peer to close and then close the TCPSocket comm\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.read_msg-Tuple{Any}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.read_msg",
    "category": "Method",
    "text": "read_msg(msg::Any)\n\nConvert msg from bytes to strings except for serialized parts.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.to_serialize-Tuple{Any}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.to_serialize",
    "category": "Method",
    "text": "to_serialize(item) -> Vector{UInt8}\n\nSerialize item.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.to_deserialize-Tuple{Array{UInt8,1}}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.to_deserialize",
    "category": "Method",
    "text": "to_deserialize(serialized_item::Vector{UInt8}) -> Any\n\nParse and deserialize serialized_item.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.pack_data-Tuple{Any,Dict{String,Any}}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.pack_data",
    "category": "Method",
    "text": "pack_data(object, data::Dict{String, Any})\n\nMerge known data into object.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.pack_object-Tuple{Any,Dict{String,Any}}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.pack_object",
    "category": "Method",
    "text": "pack_object(object, data::Dict{String, Any})\n\nReplace a DispatchNode's key with its result only if object is a known key.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.unpack_data-Tuple{Any}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.unpack_data",
    "category": "Method",
    "text": "unpack_data(object)\n\nUnpack DispatchNode objects from object. Returns the unpacked object.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.unpack_object-Tuple{Any}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.unpack_object",
    "category": "Method",
    "text": "unpack_object(object)\n\nReplace object with its key if object is a DispatchNode or else returns the original object.\n\n\n\n"
},

{
    "location": "pages/communication.html#API-1",
    "page": "Communication",
    "title": "API",
    "category": "section",
    "text": "(For Internal Use)DaskDistributedDispatcher.send_recv{T}(::TCPSocket, ::Dict{String, T})\nDaskDistributedDispatcher.send_msg(::TCPSocket, ::Union{String, Array, Dict})\nDaskDistributedDispatcher.recv_msg(::TCPSocket)\nDaskDistributedDispatcher.close_comm(::TCPSocket)\nDaskDistributedDispatcher.read_msg(::Any)\nDaskDistributedDispatcher.to_serialize(::Any)\nDaskDistributedDispatcher.to_deserialize(::Vector{UInt8})\nDaskDistributedDispatcher.pack_data(::Any, ::Dict{String, Any})\nDaskDistributedDispatcher.pack_object(::Any, ::Dict{String, Any})\nDaskDistributedDispatcher.unpack_data(::Any)\nDaskDistributedDispatcher.unpack_object(::Any)"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.Server",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.Server",
    "category": "Type",
    "text": "Server\n\nAbstract type to listen for and handle incoming messages.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.start_listening-Tuple{DaskDistributedDispatcher.Server}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.start_listening",
    "category": "Method",
    "text": "start_listening(server::Server; handler::Function=handle_comm)\n\nListen for incoming connections on a port and dispatches them to be handled.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.handle_comm-Tuple{DaskDistributedDispatcher.Server,TCPSocket}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.handle_comm",
    "category": "Method",
    "text": "handle_comm(server::Server, comm::TCPSocket)\n\nListen for incoming messages on an established connection.\n\n\n\n"
},

{
    "location": "pages/communication.html#Server-1",
    "page": "Communication",
    "title": "Server",
    "category": "section",
    "text": "(For Internal Use)DaskDistributedDispatcher.Server\nDaskDistributedDispatcher.start_listening(::DaskDistributedDispatcher.Server)\nDaskDistributedDispatcher.handle_comm(::DaskDistributedDispatcher.Server, ::TCPSocket)"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.Rpc",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.Rpc",
    "category": "Type",
    "text": "Rpc\n\nManage open socket connections to a specific address.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.Rpc-Tuple{DaskDistributedDispatcher.Address}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.Rpc",
    "category": "Method",
    "text": "Rpc(address::Address) -> Rpc\n\nManage, open, and reuse socket connections to a specific address as required.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.send_recv-Tuple{DaskDistributedDispatcher.Rpc,Dict{String,T}}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.send_recv",
    "category": "Method",
    "text": "send_recv{T}(rpc::Rpc, msg::Dict{String, T})\n\nSend msg and wait for a response.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.start_comm-Tuple{DaskDistributedDispatcher.Rpc}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.start_comm",
    "category": "Method",
    "text": "start_comm(rpc::Rpc) -> TCPSocket\n\nStart a new socket connection.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.get_comm-Tuple{DaskDistributedDispatcher.Rpc}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.get_comm",
    "category": "Method",
    "text": "get_comm(rpc::Rpc) -> TCPSocket\n\nReuse a previously open connection if available, if not, start a new one.\n\n\n\n"
},

{
    "location": "pages/communication.html#Base.close-Tuple{DaskDistributedDispatcher.Rpc}",
    "page": "Communication",
    "title": "Base.close",
    "category": "Method",
    "text": "Base.close(rpc::Rpc)\n\nClose all communications.\n\n\n\n"
},

{
    "location": "pages/communication.html#Rpc-1",
    "page": "Communication",
    "title": "Rpc",
    "category": "section",
    "text": "(For Internal Use)DaskDistributedDispatcher.Rpc\nDaskDistributedDispatcher.Rpc(::Address)\nDaskDistributedDispatcher.send_recv{T}(::DaskDistributedDispatcher.Rpc, ::Dict{String, T})\nDaskDistributedDispatcher.start_comm(::DaskDistributedDispatcher.Rpc)\nDaskDistributedDispatcher.get_comm(::DaskDistributedDispatcher.Rpc)\nDaskDistributedDispatcher.close(::DaskDistributedDispatcher.Rpc)"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.ConnectionPool",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.ConnectionPool",
    "category": "Type",
    "text": "ConnectionPool\n\nManage a limited number pool of TCPSocket connections to different addresses. Default number of open connections allowed is 512.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.ConnectionPool-Tuple{Integer}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.ConnectionPool",
    "category": "Method",
    "text": "ConnectionPool(limit::Integer=50) -> ConnectionPool\n\nReturn a new ConnectionPool which limits the total possible number of connections open to limit.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.send_recv-Tuple{DaskDistributedDispatcher.ConnectionPool,DaskDistributedDispatcher.Address,Dict{String,T}}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.send_recv",
    "category": "Method",
    "text": "send_recv{T}(pool::ConnectionPool, address::Address, msg::Dict{String, T})\n\nSend msg to address and wait for a response.\n\nReturns\n\nUnion{String, Array, Dict}: the reply received from address\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.get_comm-Tuple{DaskDistributedDispatcher.ConnectionPool,DaskDistributedDispatcher.Address}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.get_comm",
    "category": "Method",
    "text": "get_comm(pool::ConnectionPool, address::Address)\n\nGet a TCPSocket connection to the given address.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.reuse-Tuple{DaskDistributedDispatcher.ConnectionPool,DaskDistributedDispatcher.Address,TCPSocket}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.reuse",
    "category": "Method",
    "text": "reuse(pool::ConnectionPool, address::Address, comm::TCPSocket)\n\nReuse an open communication to the given address.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.collect_comms-Tuple{DaskDistributedDispatcher.ConnectionPool}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.collect_comms",
    "category": "Method",
    "text": "collect_comms(pool::ConnectionPool)\n\nCollect open but unused communications to allow opening other ones.\n\n\n\n"
},

{
    "location": "pages/communication.html#Base.close-Tuple{DaskDistributedDispatcher.ConnectionPool}",
    "page": "Communication",
    "title": "Base.close",
    "category": "Method",
    "text": "Base.close(pool::ConnectionPool)\n\nClose all communications.\n\n\n\n"
},

{
    "location": "pages/communication.html#ConnectionPool-1",
    "page": "Communication",
    "title": "ConnectionPool",
    "category": "section",
    "text": "(For Internal Use)DaskDistributedDispatcher.ConnectionPool\nDaskDistributedDispatcher.ConnectionPool(::Integer)\nDaskDistributedDispatcher.send_recv{T}(::DaskDistributedDispatcher.ConnectionPool, ::DaskDistributedDispatcher.Address, ::Dict{String, T})\nDaskDistributedDispatcher.get_comm(::DaskDistributedDispatcher.ConnectionPool, ::DaskDistributedDispatcher.Address)\nDaskDistributedDispatcher.reuse(::DaskDistributedDispatcher.ConnectionPool, ::DaskDistributedDispatcher.Address, ::TCPSocket)\nDaskDistributedDispatcher.collect_comms(::DaskDistributedDispatcher.ConnectionPool)\nDaskDistributedDispatcher.close(::DaskDistributedDispatcher.ConnectionPool)"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.BatchedSend",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.BatchedSend",
    "category": "Type",
    "text": "BatchedSend\n\nBatch messages in batches on a stream. Batching several messages at once helps performance when sending a myriad of tiny messages. Used by both the julia worker and client to communicate with the scheduler.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.BatchedSend-Tuple{TCPSocket}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.BatchedSend",
    "category": "Method",
    "text": "BatchedSend(comm::TCPSocket; interval::AbstractFloat=0.002) -> BatchedSend\n\nBatch messages in batches on comm. We send lists of messages every interval milliseconds.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.background_send-Tuple{DaskDistributedDispatcher.BatchedSend}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.background_send",
    "category": "Method",
    "text": "background_send(batchedsend::BatchedSend)\n\nSend the messages in batchsend.buffer every interval milliseconds.\n\n\n\n"
},

{
    "location": "pages/communication.html#DaskDistributedDispatcher.send_msg-Tuple{DaskDistributedDispatcher.BatchedSend,Dict{String,T}}",
    "page": "Communication",
    "title": "DaskDistributedDispatcher.send_msg",
    "category": "Method",
    "text": "send_msg{T}(batchedsend::BatchedSend, msg::Dict{String, T})\n\nSchedule a message for sending to the other side. This completes quickly and synchronously.\n\n\n\n"
},

{
    "location": "pages/communication.html#Base.close-Tuple{DaskDistributedDispatcher.BatchedSend}",
    "page": "Communication",
    "title": "Base.close",
    "category": "Method",
    "text": "Base.close(batchedsend::BatchedSend)\n\nTry to send all remaining messages and then close the connection.\n\n\n\n"
},

{
    "location": "pages/communication.html#BatchedSend-1",
    "page": "Communication",
    "title": "BatchedSend",
    "category": "section",
    "text": "(For Internal Use)DaskDistributedDispatcher.BatchedSend\nDaskDistributedDispatcher.BatchedSend(::TCPSocket)\nDaskDistributedDispatcher.background_send(::DaskDistributedDispatcher.BatchedSend)\nDaskDistributedDispatcher.send_msg{T}(::DaskDistributedDispatcher.BatchedSend, ::Dict{String, T})\nDaskDistributedDispatcher.close(::DaskDistributedDispatcher.BatchedSend)"
},

]}
