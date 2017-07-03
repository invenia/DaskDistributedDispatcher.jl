# DaskDistributedDispatcher.jl

```@meta
CurrentModule = DaskDistributedDispatcher
```

DaskDistributedDispatcher integrates `Dispatcher.jl` with the python `dask.distributed` scheduler service.

## Overview

[`Dispatcher.jl`](https://invenia.github.io/Dispatcher.jl/latest/index.html) builds the graph of julia computations and submits jobs via the julia client to the  [`dask.distributed scheduler`](https://distributed.readthedocs.io/), which is in charge of determining when and where to schedule jobs on the julia workers. Thus, the computations are scheduled and executed efficiently.

## Frequently Asked Questions

> How can the python `dask.distributed` scheduler be used for julia computations?

The `dask.distributed` scheduler can be used in a julia workflow environment since it is language agnostic (no information that passes in or out of it is Python-specific). Instead the scheduler communicates with the workers/clients entirely using msgpack and long bytestrings.

## Documentation Contents

```@contents
Pages = ["pages/manual.md", "pages/api.md"]
```
