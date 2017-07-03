# API

## Client

```@docs
Client
Client(::String)
submit(::Client, ::Dispatcher.Op; ::Array)
result(::Client, ::Dispatcher.Op)
cancel(::Client, ::Array{Dispatcher.Op})
gather(::Client, ::Array{Dispatcher.Op})
shutdown(::Client)
default_client()
```

## Worker

```@docs
Worker
Worker(::String)
address(::Worker)
show(::IO, ::Worker)
```
