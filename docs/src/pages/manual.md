# Manual

## Motivation

The primary reason for integrating the [`dask.distributed`](https://distributed.readthedocs.io/en/latest/index.html) scheduler with [`Dispatcher.jl`](https://invenia.github.io/Dispatcher.jl/stable/) is to be able to guarantee a stronger degree of effiency for computations run and to allow for fluctuating worker resources. (Note that removing workers from the worker pool may cause errors when fetching results. Only remove workers once you no longer need access to their information.)

Using the `dask-scheduler` and executing compuations in a distributed manner can
[`add overhead for simple tasks`]
(https://distributed.readthedocs.io/en/latest/efficiency.html). Consider using an
[`AsyncExecuter`]
(https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#AsyncExecutor-1) or
[`ParallelExecuter`]
(https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#ParallelExecutor-1) if
possible. The advantage that using the `dask-scheduler` has is that it schedules
computations in a manner that is [`short-term-efficient and long-term-fair`]
(https://distributed.readthedocs.io/en/latest/scheduling-policies.html).

## Design

The key components of this system are:

* the [`dask-scheduler`](https://distributed.readthedocs.io/en/latest/scheduling-policies.html) process that schedules computations and manages state
* a julia [`Client`](@ref) or [`DaskExecutor`](@ref) that submits work to the scheduler
* julia [`Worker`](@ref)s that accept instructions from the scheduler, fetch dependencies, execute compuations, store data, and communicate state to the scheduler

## Prerequisites

Python 2.7 or 3.5+ and the Python package `dask.distributed` need to be installed ([`instructions here`](http://distributed.readthedocs.io/en/latest/install.html)) before using this package.
The minimum required version of the dask distributed package is >= v1.18.1.

### macOS and Python 2

Currently this package will fail to process some workloads when using Python 2 on macOS.
Use Python 3 to run `dask-scheduler`; it does not need to be the same Python as the one used for [PyCall.jl](https://github.com/JuliaPy/PyCall.jl) or [Conda.jl](https://github.com/JuliaPy/Conda.jl).

## Setup

First, start a dask-scheduler process in a terminal:

```
$ dask-scheduler
Scheduler started at 127.0.0.1:8786
```

Then, in a julia session, set up a [`cluster`](https://docs.julialang.org/en/stable/manual/parallel-computing/#clustermanagers) of julia processes and initialize the workers by providing them with the `dask-scheduler`'s tcp address:

```julia
using DaskDistributedDispatcher

addprocs(3)
@everywhere using DaskDistributedDispatcher

for i in workers()
    @spawnat i Worker("127.0.0.1:8786")
end
```

## Usage

Submit [`DispatchNode`](https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#DispatchNode-1)s units of computation that can be run to the [`DaskExecutor`](@ref) (which will relay them to the `dask-scheduler` to be scheduled and executed on a [`Worker`](@ref)):

```julia
using Dispatcher
using ResultTypes

data = [1, 2, 3]

a = @op 1 + 2
x = @op a + 3
y = @op a + 1

result = @op x * y

executor = DaskExecutor("127.0.0.1:8786")
(run_result,) = run!(executor, [result])

run_future = unwrap(run_result)
@assert fetch(run_future) == 24
```

**Note**: There must be at least one [`Worker`](@ref) running or else `run!` will hang indefinetely. Also, if the dask-scheduler is running on the same machine as the [`DaskExecutor`](@ref) and on its default port (8786), the address can be ommitted when initializing [`Worker`](@ref)s and [`DaskExecutor`](@ref)s.

```julia
@spawn Worker()

executor = DaskExecutor()
```

See [`DaskExecutor`](@ref) and [`Dispatcher.jl`](https://invenia.github.io/Dispatcher.jl/latest/pages/manual.html) for more usage information.

## Additional notes

Using a `Channel` and/or `Future` in computations submitted to the `DaskExecutor` is not supported. Instead use a [`DeferredChannel` or `DeferredFuture`](https://github.com/invenia/DeferredFutures.jl).

It is possible to bypass the [`DaskExecutor`](@ref) by accessing its `client` variable for more advanced workflows such as cancelling previously submitted computations or asking the scheduler to replicate data across all workers. See [`Client`](@ref) for more information.

When done your computations, to get the `dask-scheduler` to reset and delete all previously computed values without restarting the [`Worker`](@ref)s and the `dask-scheduler` call:

```julia
reset!(executor)
```





