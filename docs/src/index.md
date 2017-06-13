# DaskDistributedDispatcher.jl

```@meta
CurrentModule = DaskDistributedDispatcher
```

DaskDistributedDispatcher integrates `Dispatcher.jl` with the python `dask.distributed` scheduler service.

## Overview

[`Dispatcher.jl`](https://invenia.github.io/Dispatcher.jl/latest/index.html) builds the graph of julia computations and submits jobs to the  [`dask.distributed`](https://distributed.readthedocs.io/) scheduler, which then determines when and where to schedule them. Thus, the computations can be scheduled and executed with a greater guarantee of effiency.


```@contents
Pages = ["pages/manual.md", "pages/api.md"]
```
