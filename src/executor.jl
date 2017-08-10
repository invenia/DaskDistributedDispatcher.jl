# ONLY NECESSARY ON 0.5
if VERSION < v"0.6.0-dev.1515"
    function asyncmap(f, c...; ntasks=0)
        collect(Base.AsyncGenerator(f, c...; ntasks=ntasks))
    end
else
    asyncmap = Base.asyncmap
end

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
    DaskExecutor(scheduler_address::String="\$(getipaddr()):8786")

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
`dask-scheduler`'s address or else [`run!`]
(https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#Dispatcher.run!-Tuple{Dispatcher.Executor,Dispatcher.DispatchContext,AbstractArray{T<:Dispatcher.DispatchNode,N},AbstractArray{S<:Dispatcher.DispatchNode,N}}
) will hang indefinetely.

## Examples

* Running asynchronously:

```julia
# Reminder: make sure the dask-scheduler is running
using DaskDistributedDispatcher
using Dispatcher
using ResultTypes

Worker()

exec = DaskExecutor()
ctx = DispatchContext()
n1 = add!(ctx, Op(()->3))
n2 = add!(ctx, Op(()->4))

results = run!(exec, ctx)

fetch(unwrap(results[1]))  # 3
fetch(unwrap(results[2]))  # 4
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
ctx = DispatchContext()
n1 = add!(ctx, Op(()->3))
n2 = add!(ctx, Op(()->4))

results = run!(exec, ctx)

fetch(unwrap(results[1]))  # 3
fetch(unwrap(results[2]))  # 4
```

To delete all previously computed information from the workers:

```julia
reset(exec)
```

## Advanced Workflows

It is possible to bypass the [`DaskExecutor`](@ref) and use the [`Client`](@ref) directly to
submit compuations, cancel previously scheduled [`DispatchNode`]
(https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#DispatchNode-1)s, gather
results, or replicate data across all workers. See [`Client`](@ref) for more details. It is
recommened to start with a [`DaskExecutor`](@ref) and access its `client` field if needed
later on.
"""
function DaskExecutor(scheduler_address::String="$(getipaddr()):8786")
    client = Client(scheduler_address)

    return DaskExecutor(0, Function[], client, scheduler_address)
end


"""
    dispatch!(exec::DaskExecutor, ctx::DispatchContext; throw_error=true) -> Vector

The default `dispatch!` method uses `asyncmap` over all nodes in the context to call
`dispatch!(exec, node)`. These `dispatch!` calls for each node are wrapped in various retry
and error handling methods.

## Wrapping Details

1. All nodes are wrapped in a try catch which waits on the value returned from the
   `dispatch!(exec, node)` call. Any errors are caught and used to create [`DependencyError`]
   (https://invenia.github.io/Dispatcher.jl/latest/pages/api.html#Dispatcher.DependencyError)s
   which are thrown. If no errors are produced then the node is returned.

   **NOTE**: All errors thrown by trying to run `dispatch!(exec, node)` are wrapped in a
   `DependencyError`.

2. By default the [`DaskExecutor`](@ref) has no [`retry_on`](@ref) functions since retrying
   failed `ops` is not explicitly supported by the dask-scheduler.

3. A node may enter a failed state if it exits the retry wrapper with an exception.
   In the situation where a node has entered a failed state and the node is an `Op` then
   the `op.result` is set to the `DependencyError`, signifying the node's failure to any
   dependent nodes.
   Finally, if `throw_error` is true then the `DependencyError` will be immediately thrown
   in the current process without allowing other nodes to finish.
   If `throw_error` is false then the `DependencyError` is not thrown and it will be
   returned in the array of passing and failing nodes.

## Arguments

* `exec::DaskExecutor`: the executor we're running
* `ctx::DispatchContext`: the context of nodes to run

## Keyword Arguments

* `throw_error::Bool=true`: whether or not to throw the `DependencyError` for failed nodes

## Returns

* `Vector{Union{DispatchNode, DependencyError}}`: a list of `DispatchNode`s or
  `DependencyError`s for failed nodes

## Throws

* `dispatch!` has the same behaviour on exceptions as `asyncmap` and `pmap`.
  In 0.5 this will throw a `CompositeException` containing `DependencyError`s, while
  in 0.6 this will simply throw the first `DependencyError`.
"""
function Dispatcher.dispatch!(exec::DaskExecutor, ctx::DispatchContext; throw_error=true)
    ns = ctx.graph.nodes

    function run_inner!(id::Int)
        node = ns[id]
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
        return ns[id]
    end

    """
        on_error_inner!(err::Exception)

    Log and throw an exception.
    This is the default behaviour.
    """
    function on_error_inner!(err::Exception)
        warn(logger, "Unhandled Error: $err")
        throw(err)
    end

    """
        on_error_inner!(err::DependencyError) -> DependencyError

    When a dependency error occurs while attempting to run a node, put that dependency error
    in that node's result.
    Throw the error if `dispatch!` was called with `throw_error=true`, otherwise returns the
    error.
    """
    function on_error_inner!(err::DependencyError)
        notice(logger, "Handling Error: $(summary(err))")

        node = ctx.graph.nodes[err.id]
        if isa(node, Union{Op, IndexNode})
            Dispatcher.reset!(node.result)
            put!(node.result, err)
        end

        if throw_error
            throw(err)
        end

        return err
    end

    retry_args = @static if VERSION < v"0.6.0-dev.2042"
        (Dispatcher.allow_retry(retry_on(exec)), retries(exec), Base.DEFAULT_RETRY_MAX_DELAY)
    else
        (ExponentialBackOff(; n=retries(exec)), Dispatcher.allow_retry(retry_on(exec)))
    end

    function reset_all!(id::Int)
        node = ns[id]
        if isa(node, Union{Op, IndexNode})
            Dispatcher.reset!(ns[id].result)
        end
    end

    f1 = Dispatcher.wrap_on_error(
        Dispatcher.wrap_retry(
            reset_all!,
            retry_args...,
        ),
        on_error_inner!
    )

    f2 = Dispatcher.wrap_on_error(
        Dispatcher.wrap_retry(
            run_inner!,
            retry_args...,
        ),
        on_error_inner!
    )

    len = length(ctx.graph.nodes)
    info(logger, "Executing $len graph nodes.")

    # Reset nodes before any are submitted to avoid race conditions when the node is reset
    # after it has been completed.
    asyncmap(f1, 1:len; ntasks=div(len * 3, 2))

    res = asyncmap(f2, 1:len; ntasks=div(len * 3, 2))

    info(logger, "All $len nodes executed.")

    return res
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
