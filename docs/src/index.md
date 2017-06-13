# DaskDistributedDispatcher.jl

```@meta
CurrentModule = DaskDistributedDispatcher
```

DaskDistributedDispatcher integrates `Dispatcher.jl` with the python `dask.distributed` scheduler service.

## Overview
`Dispatcher.jl` builds the graph of julia computations and submits the jobs to the `dask.distributed` scheduler, which then determines when and where to schedule them. Thus, the computations can be shceduled and executed in as efficient a manner as possible.