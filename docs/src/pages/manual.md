# Manual

## Motivation

Using the `dask.distributed` sheduler with `Dispatcher.jl` is designed to guarantee a stronger degree of effiency and allow for fluctuating worker resources. This also avoids pre-allocating tasks.

The `dask.distributed` scheduler can be used in a julia workflow enviroment since it communicates entirely using `msgpack` and long bytestrings, making it language agnostic (no information that passes in or out of it is Python-specific).

## Design 

The key components of this system are:
<ul>
<li>the dask-scheduler process that schedules computations and manages state</li>
<li>a julia client used by `Dispatcher.jl` that submits work to the scheduler</li>
<li>julia workers that accept instructions from the scheduler, fetch dependencies, execute compuations, store data, and communicates state to the scheduler</li>
</ul>

## Usage

To use this package you need to [`install Dask.Distributed`](http://distributed.readthedocs.io/en/latest/install.html) and start a dask-scheduler process:

```
$ dask-scheduler
Scheduler started at 127.0.0.1:8786
```

Then start a julia process and startup a [`cluster`](https://docs.julialang.org/en/stable/manual/parallel-computing/#clustermanagers) of julia client/workers providing them the scheduler's address:

```julia
using DaskDistributedDispatcher
addprocs()
@everywhere using DaskDistributedDispatcher
client = Client("127.0.0.1:8786")
@spawn worker = Worker("127.0.0.1:8786")
@spawn worker = Worker("127.0.0.1:8786")
```

Then you can submit Dispatcher `Op`s which represent units of compuation that can be run (a function call on some arguments) to the client which will relay it to the dask-scheduler and then be executed on a worker.

```julia
op = Dispatcher.Op(Int, 2.0)
submit(client, op)
result = result(client, op)
```
