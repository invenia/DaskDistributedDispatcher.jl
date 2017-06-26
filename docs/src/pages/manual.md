# Manual

## Motivation

The primary reason for integrating the `dask.distributed` sheduler with `Dispatcher.jl` is to be able to guarantee a stronger degree of effiency for computations run on `Dispatcher` and to allow for fluctuating worker resources.

## Design 

The key components of this system are:

* the dask-scheduler process that schedules computations and manages state
* a julia client used by `Dispatcher.jl` that submits work to the scheduler
* julia workers that accept instructions from the scheduler, fetch dependencies, execute compuations, store data, and communicate state to the scheduler

## Setup

To use this package you need to also [`install Dask.Distributed`](http://distributed.readthedocs.io/en/latest/install.html).

## Usage

First start a dask-scheduler process:

```
$ dask-scheduler
Scheduler started at 127.0.0.1:8786
```

Then, start a julia process and set up a [`cluster`](https://docs.julialang.org/en/stable/manual/parallel-computing/#clustermanagers) of julia client/workers providing them the scheduler's address:

```julia
using DaskDistributedDispatcher
addprocs()
@everywhere using DaskDistributedDispatcher
client = Client("127.0.0.1:8786")
@spawn worker = Worker("127.0.0.1:8786")
@spawn worker = Worker("127.0.0.1:8786")
```

Then, you can submit Dispatcher `Ops` units of computation that can be run to the client which will relay it to the dask-scheduler to be scheduled and executed on a worker.

```julia
op = Dispatcher.Op(Int, 2.0)
submit(client, op)
result = result(client, op)
```
