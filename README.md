# DaskDistributedDispatcher

[![Build Status](https://travis-ci.org/invenia/DaskDistributedDispatcher.jl.svg?branch=master)](https://travis-ci.org/invenia/DaskDistributedDispatcher.jl)
[![Build status](https://ci.appveyor.com/api/projects/status/2t52xoxhct02yevh/branch/master?svg=true)](https://ci.appveyor.com/project/nicoleepp/daskdistributeddispatcher-jl/branch/master)
[![codecov](https://codecov.io/gh/invenia/DaskDistributedDispatcher.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/invenia/DaskDistributedDispatcher.jl)

DaskDistributedDispatcher integrates `Dispatcher.jl` with the python `dask.distributed` scheduler service.

Documentation: [![](https://img.shields.io/badge/docs-latest-blue.svg)](https://invenia.github.io/DaskDistributedDispatcher.jl/latest)

## Overview

[`Dispatcher.jl`](https://invenia.github.io/Dispatcher.jl/latest/index.html) builds the graph of julia computations and submits jobs to the  [`dask.distributed`](https://distributed.readthedocs.io/) scheduler, which then determines when and where to schedule them. Thus, the computations can be scheduled and executed with a greater guarantee of effiency.

## Frequently Asked Questions

> Isn't `dask.distributed` written in python?

The `dask.distributed` scheduler can be used in a julia workflow environment since it is language agnostic (no information that passes in or out of it is Python-specific) but instead it communicates entirely using msgpack and long bytestrings.