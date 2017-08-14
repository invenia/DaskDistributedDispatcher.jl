# DaskDistributedDispatcher

[![Build Status](https://travis-ci.org/invenia/DaskDistributedDispatcher.jl.svg?branch=master)](https://travis-ci.org/invenia/DaskDistributedDispatcher.jl)
[![Build status](https://ci.appveyor.com/api/projects/status/2t52xoxhct02yevh/branch/master?svg=true)](https://ci.appveyor.com/project/nicoleepp/daskdistributeddispatcher-jl/branch/master)
[![codecov](https://codecov.io/gh/invenia/DaskDistributedDispatcher.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/invenia/DaskDistributedDispatcher.jl)

DaskDistributedDispatcher integrates `Dispatcher.jl` with the python `dask.distributed` scheduler service.

Documentation: [![](https://img.shields.io/badge/docs-latest-blue.svg)](https://invenia.github.io/DaskDistributedDispatcher.jl/latest)

## Overview

[`Dispatcher.jl`](https://invenia.github.io/Dispatcher.jl/latest/index.html) builds the graph of julia computations and submits jobs via the julia client to the  [`dask.distributed scheduler`](https://distributed.readthedocs.io/), which is in charge of determining when and where to schedule jobs on the julia workers. Thus, the computations can be scheduled and executed efficiently.

## Frequently Asked Questions

> How can the python `dask.distributed` scheduler be used for julia computations?

The `dask.distributed` scheduler can be used in a julia workflow environment since it is language agnostic (no information that passes in or out of it is Python-specific). Instead the scheduler communicates with the workers/clients entirely using msgpack and long bytestrings. More information on the protocol used is [`here`](http://distributed.readthedocs.io/en/latest/protocol.html).

## License

DaskDistributedDispatcher.jl is provided under the [Mozilla Public License 2.0](LICENSE.md).
