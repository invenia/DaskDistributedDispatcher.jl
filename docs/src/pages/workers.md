# Workers

Julia workers were developed that integrate with the python [`dask-scheduler`](http://distributed.readthedocs.io/en/latest/setup.html), and hence follow many of the same patterns that the python [`dask-workers`] (http://distributed.readthedocs.io/en/latest/worker.html) do.


## Notable Differences

* The julia workers don't execute computations in a thread pool but rather do so asynchronously. The recommended way to setup the workers is to use `addprocs` and spawn at least one `Worker` per julia process added in the cluster.

* Currently the julia workers do not support specifying `resources` needed by computations or spilling excess data onto disk.


## Tasks 

Usually computations submitted to a worker go through task states in the following order:

    waiting -> ready -> executing -> memory

Computations that result in errors being thrown are caught and the error is saved in memory.
Workers communicate between themselves to gather dependencies and with the dask-scheduler.


## API

```@docs
Worker
Worker(::String)
shutdown(::Array{Address, 1})
show(::IO, ::Worker)
```

## Internals

```@docs
DaskDistributedDispatcher.start_worker(::Worker)
DaskDistributedDispatcher.register_worker(::Worker)
DaskDistributedDispatcher.handle_comm(::Worker, ::TCPSocket)
DaskDistributedDispatcher.close(::Worker)
DaskDistributedDispatcher.get_data(::Worker; ::Array, ::String)
DaskDistributedDispatcher.gather(::Worker; ::Dict)
DaskDistributedDispatcher.update_data(::Worker; ::Dict, ::String)
DaskDistributedDispatcher.delete_data(::Worker; ::Array, ::String)
DaskDistributedDispatcher.terminate(::Worker; ::String)
DaskDistributedDispatcher.get_keys(::Worker)
DaskDistributedDispatcher.add_task(::Worker; ::String, ::Array, ::Dict, ::Dict, ::String, ::Dict, ::Union{String, Array{UInt8,1}}, ::Union{String, Array{UInt8,1}}, ::Union{String, Array{UInt8,1}}, ::Union{String, Array{UInt8,1}})
DaskDistributedDispatcher.release_key(::Worker; ::String, ::String, ::String)
DaskDistributedDispatcher.release_dep(::Worker, ::String)
DaskDistributedDispatcher.ensure_computing(::Worker)
DaskDistributedDispatcher.execute(::Worker, ::String)
DaskDistributedDispatcher.put_key_in_memory(::Worker, ::String, ::Any; ::Bool)
DaskDistributedDispatcher.ensure_communicating(::Worker)
DaskDistributedDispatcher.gather_dep(::Worker, ::String, ::String, ::Set; ::String)
DaskDistributedDispatcher.handle_missing_dep(::Worker, ::Set{String})
DaskDistributedDispatcher.bad_dep(::Worker, ::String)
DaskDistributedDispatcher.update_who_has(::Worker, ::Dict{String, Array{Any, 1}})
DaskDistributedDispatcher.select_keys_for_gather(::Worker, ::String, ::String)
DaskDistributedDispatcher.gather_from_workers(::Dict, ::DaskDistributedDispatcher.ConnectionPool)
DaskDistributedDispatcher.transition(::Worker, ::String, ::String; kwargs...)
DaskDistributedDispatcher.transition_dep(::Worker, ::String, ::String; kwargs...)
DaskDistributedDispatcher.send_task_state_to_scheduler(::Worker, ::String)
DaskDistributedDispatcher.deserialize_task(::Union{String, Array}, ::Union{String, Array}, ::Union{String, Array})
DaskDistributedDispatcher.apply_function(::Any, ::Any, ::Any)
```

