# Manual

## Motivation

The primary reason for integrating the `dask.distributed` sheduler with `Dispatcher.jl` is to be able to guarantee a stronger degree of effiency for computations run on `Dispatcher` and to allow for fluctuating worker resources.

## Design 

The key components of this system are:

* the dask-scheduler process that schedules computations and manages state
* a julia client used by `Dispatcher.jl` that submits work to the scheduler
* julia workers that accept instructions from the scheduler, fetch dependencies, execute compuations, store data, and communicate state to the scheduler

In order to avoid redundant computations, the client will reuse previously computed results for identical operations.

## Setup

To use this package you also need to [`install Dask.Distributed`](http://distributed.readthedocs.io/en/latest/install.html).

## Usage

First, start a dask-scheduler process:

```
$ dask-scheduler
Scheduler started at 127.0.0.1:8786
```

Then, start a julia session and set up a [`cluster`](https://docs.julialang.org/en/stable/manual/parallel-computing/#clustermanagers) of julia client/workers, providing them the scheduler's address:

```julia
using DaskDistributedDispatcher
client = Client("127.0.0.1:8786")

addprocs()
@everywhere using DaskDistributedDispatcher

@spawn worker = Worker("127.0.0.1:8786")
@spawn worker = Worker("127.0.0.1:8786")
```

You can then submit Dispatcher `DispatchNode` units of computation that can be run to the client (which will relay it to the dask-scheduler to be scheduled and executed on a worker):

```julia
using Dispatcher

op = Op(Int, 2.0)
submit(client, op)
result = result(client, op)
```

Alternatively, you can get the results directly from the `Op`:

```julia
result = fetch(op)
```

Previously submitted `Ops` can be cancelled by calling:

```julia
cancel(client, [op])
```

If needed, you can specify which worker(s) to run the computations on by returning the worker's address when starting a new worker:

```julia
using DaskDistributedDispatcher
client = Client("127.0.0.1:8786")

pnums = addprocs(1)
@everywhere using DaskDistributedDispatcher

worker_address = @fetchfrom pnums[1] begin
    worker = Worker("127.0.0.1:8786")
    return worker.address
end

op = Op(Int, 1.0)
submit(client, op, workers=[worker_address])
result = result(client, op)
```

The worker's address can also be used to shutdown the worker remotely:

```julia
shutdown([worker_address])
```

Currently, if the `Op` submitted to the client results in an error, the result of the `Op` will then be a string representation of the error that occurred on the worker.

```julia
julia> op = Op(Int, 2.1)
julia> submit(client, op)
julia> result = result(client, op)
"error"=>"InexactError"
```
