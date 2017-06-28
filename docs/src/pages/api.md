# API

## Client

```@docs
Client
Client(::String)
submit(::Client, ::Dispatcher.Op)
result(::Client, ::Dispatcher.Op)
cancel(::Client, ::Array)
gather(::Client, ::Array)
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
