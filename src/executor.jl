"""
`DaskExecutor` is an [`Executor`]
(https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#Dispatcher.Executor)  which
executes julia computations as scheduled by the python [`dask-scheduler`]
(https://distributed.readthedocs.io/en/latest/scheduling-policies.html). It can run
computations both asynchronously or in parallel (if [`Worker`](@ref)s are started on a julia
cluster instead).

`DaskExecutor`'s [`dispatch!(::DaskExecutor, ::Dispatcher.DispatchNode)`](@ref) method will
complete as long as there are no cycles in the computation graph, the [`dask-scheduler`]
(https://distributed.readthedocs.io/en/latest/scheduling-policies.html) remains
online, and there is at least one [`Worker`](@ref) that is listening to the
[`dask-scheduler`](https://distributed.readthedocs.io/en/latest/scheduling-policies.html).
"""
type DaskExecutor <: Executor
    retries::Int
    retry_on::Vector{Function}
    client::Client
    scheduler_address::String
end

"""
    DaskExecutor(scheduler_address::String="127.0.0.1:8786")

Return a new [`DaskExecutor`](@ref). The `scheduler_address` only needs to be included if
the [`dask-scheduler`]
(https://distributed.readthedocs.io/en/latest/setup.html)  is running on a
different machine or not on it's default port (8786).

**NOTE**: A [`dask-scheduler`](https://distributed.readthedocs.io/en/latest/setup.html) must
be running at all times or the [`DaskExecutor`](@ref) execution will fail. If the scheduler
is taken offline during execution for some reason, any remaining operations will fail to
complete. Start a [`dask-scheduler`]
(https://distributed.readthedocs.io/en/latest/setup.html) from a terminal by typing
`dask-scheduler`:

```
\$ dask-scheduler
Start scheduler at 192.168.0.1:8786
```

## Prerequisites

* python 2.7 or 3.5
* the python dask.distributed package ([`instructions for install here`]
  (http://distributed.readthedocs.io/en/latest/install.html))

Note that using the `dask-scheduler` and executing compuations in a distributed manner can
[`add overhead for simple tasks`]
(https://distributed.readthedocs.io/en/latest/efficiency.html). Consider using an
[`AsyncExecuter`]
(https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#AsyncExecutor-1) or
[`ParallelExecuter`]
(https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#ParallelExecutor-1) if
possible. The advantage that using the `dask-scheduler` has is that it schedules
computations in a manner that is [`short-term-efficient and long-term-fair`]
(https://distributed.readthedocs.io/en/latest/scheduling-policies.html).

## Usage

The [`DaskExecutor`](@ref) can run both asynchronously with the [`Worker`](@ref)s, or in
parallel if [`Worker`](@ref)s are spawned on separate julia processes in a cluster.

**NOTE**: Users must startup at least one [`Worker`](@ref) by pointing it to the
`dask-scheduler`'s address or else `run!` will hang indefinetely.

## Examples

* Running asynchronously:

```julia
# Reminder: make sure the dask-scheduler is running
using DaskDistributedDispatcher
using Dispatcher
using ResultTypes

Worker()

exec = DaskExecutor()

a = Op(()->3)
b = Op(()->4)
c = Op(max, a, b)

results = run!(exec, DispatchGraph(c))

fetch(unwrap(results[1]))  # 4
```

* Running in parallel:

```julia
# Reminder: make sure the dask-scheduler is running
using DaskDistributedDispatcher
using Dispatcher
using ResultTypes

addprocs(3)
@everywhere using DaskDistributedDispatcher

for i in 1:3
    cond = @spawn Worker()
    wait(cond)
end

exec = DaskExecutor()

a = Op(()->3)
b = Op(()->4)
c = Op(max, a, b)

results = run!(exec, DispatchGraph(c))

fetch(unwrap(results[1]))  # 4
```

To delete all previously computed information from the workers:

```julia
reset!(exec)
```

## Advanced Workflows

It is possible to bypass the [`DaskExecutor`](@ref) and use the [`Client`](@ref) directly to
submit compuations, cancel previously scheduled [`DispatchNode`]
(https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#DispatchNode-1)s, gather
results, or replicate data across all workers. See [`Client`](@ref) for more details. It is
recommened to start with a [`DaskExecutor`](@ref) and access its `client` field if needed
later on.
"""
function DaskExecutor(scheduler_address::String="127.0.0.1:8786")
    client = Client(scheduler_address)

    return DaskExecutor(0, Function[], client, scheduler_address)
end

"""
    reset!(exec::DaskExecutor)

Restarts the executor's [`Client`](@ref), which tells the scheduler to delete previously
computed data since it is not needed anymore. The scheduler, in turn, signals this to the
workers.
"""
function reset!(exec::DaskExecutor)
    shutdown(exec.client)
    exec.client = Client(exec.scheduler_address)
end

"""
    run_inner_node!(exec::DaskExecutor, node::DispatchNode, id::Int)

Submit the `DispatchNode` at position `id` in the `DispatchGraph` for scheduling and
execution. Any error thrown during the node's execution is caught and wrapped in a
`DependencyError`.
"""
function Dispatcher.run_inner_node!(exec::DaskExecutor, node::DispatchNode, id::Int)
    desc = summary(node)
    info(logger, "Node $id ($desc): running.")

    dispatch!(exec, node)
    value = fetch(node)  # Wait for node to complete

    if isa(value, Pair) && isa(value[1], Exception)
        err = value.first
        traceback = value.second

        debug(logger, "Node $id: errored with $value")

        dep_err = Dispatcher.DependencyError(err, traceback, id)
        throw(dep_err)
    else
        info(logger, "Node $id ($desc): complete.")
    end
end

"""
    retries(exec::DaskExecutor) -> Int

Return the number of retries per node.
"""
retries(exec::DaskExecutor) = exec.retries

"""
    retry_on(exec::DaskExecutor) -> Vector{Function}

Return the array of retry conditions.
"""
retry_on(exec::DaskExecutor) = exec.retry_on

"""
    dispatch!(exec::DaskExecutor, node::Dispatcher.DispatchNode) -> Future

`dispatch!` takes the `DaskExecutor` and a [`DispatchNode`]
(https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#Dispatcher.DispatchNode) to
run and submits the [`DispatchNode`]
(https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#Dispatcher.DispatchNode) to
the [`Client`](@ref) for scheduling.

This is the defining method of `DaskExecutor`.
"""
function dispatch!(exec::DaskExecutor, node::DispatchNode)
    submit(exec.client, node)
end
