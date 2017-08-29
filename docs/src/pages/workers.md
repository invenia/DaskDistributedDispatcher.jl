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
shutdown(::Vector{Address})
show(::IO, ::Worker)
```

## Internals

```@docs
DaskDistributedDispatcher.start(::Worker)
DaskDistributedDispatcher.register(::Worker)
DaskDistributedDispatcher.handle_comm(::Worker, ::TCPSocket)
DaskDistributedDispatcher.close(::Worker)
DaskDistributedDispatcher.get_data(::Worker)
DaskDistributedDispatcher.gather(::Worker)
DaskDistributedDispatcher.update_data(::Worker)
DaskDistributedDispatcher.delete_data(::Worker)
DaskDistributedDispatcher.terminate(::Worker)
DaskDistributedDispatcher.get_keys(::Worker)
DaskDistributedDispatcher.add_task(::Worker)
DaskDistributedDispatcher.release_key(::Worker)
DaskDistributedDispatcher.release_dep(::Worker, ::String)
DaskDistributedDispatcher.ensure_computing(::Worker)
DaskDistributedDispatcher.execute(::Worker, ::String)
DaskDistributedDispatcher.put_key_in_memory(::Worker, ::String, ::Any)
DaskDistributedDispatcher.ensure_communicating(::Worker)
DaskDistributedDispatcher.gather_dep(::Worker, ::String, ::String, ::Set{String})
DaskDistributedDispatcher.handle_missing_dep(::Worker, ::Set{String})
DaskDistributedDispatcher.update_who_has(::Worker, ::Dict{String, Vector{String}})
DaskDistributedDispatcher.select_keys_for_gather(::Worker, ::String, ::String)
DaskDistributedDispatcher.gather_from_workers(::Dict{String, Vector{String}}, ::DaskDistributedDispatcher.ConnectionPool)
DaskDistributedDispatcher.transition(::Worker, ::String, ::Symbol)
DaskDistributedDispatcher.transition_dep(::Worker, ::String, ::Symbol)
DaskDistributedDispatcher.send_task_state_to_scheduler(::Worker, ::String)
DaskDistributedDispatcher.deserialize_task(::Vector{UInt8}, ::Vector{UInt8}, ::Vector{UInt8}, ::Vector{UInt8})
DaskDistributedDispatcher.apply_function(::String, ::Base.Callable, ::Tuple, ::Vector{Any})
```

