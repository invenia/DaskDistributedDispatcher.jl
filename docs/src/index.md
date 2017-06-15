# DaskDistributedDispatcher.jl

```@meta
CurrentModule = DaskDistributedDispatcher
```

DaskDistributedDispatcher integrates `Dispatcher.jl` with the python `dask.distributed` scheduler service.

## Overview

[`Dispatcher.jl`](https://invenia.github.io/Dispatcher.jl/latest/index.html) builds the graph of julia computations and submits jobs to the  [`dask.distributed`](https://distributed.readthedocs.io/) scheduler, which then determines when and where to schedule them. Thus, the computations can be scheduled and executed with a greater guarantee of effiency.

## Frequently Asked Questions

> Isn't `dask.distributed` written in python?

The `dask.distributed` scheduler can be used in a julia workflow enviroment since it is language agnostic (no information that passes in or out of it is Python-specific) but instead it communicates entirely using msgpack and long bytestrings.

## Documentation Contents

```@contents
Pages = ["pages/manual.md", "pages/api.md"]
```
