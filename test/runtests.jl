using DaskDistributedDispatcher
using Base.Test
using Dispatcher
using Memento
using ResultTypes

import DaskDistributedDispatcher:
    read_msg,
    send_msg,
    parse_address,
    to_serialize,
    to_deserialize,
    pack_data,
    unpack_data,
    send_to_scheduler,
    ConnectionPool,
    BatchedSend,
    send_recv,
    recv_msg,
    to_key,
    Server,
    start_listening

const host_ip = getipaddr()
const host = string(host_ip)

inline_flag = Base.JLOptions().can_inline == 1 ? `` : `--inline=no`
cov_flag = ``
if Base.JLOptions().code_coverage == 1
    cov_flag = `--code-coverage=user`
elseif Base.JLOptions().code_coverage == 2
    cov_flag = `--code-coverage=all`
end

const LOG_LEVEL = "debug"      # could also be "debug", "notice", "warn", etc

Memento.config(LOG_LEVEL; fmt="[{level} | {name}]: {msg}")
const logger = get_logger(current_module())

function test_addprocs(n::Int)
    pnums = addprocs(
        n;
        exeflags=`$cov_flag $inline_flag --color=yes --check-bounds=yes --startup-file=no`
    )
    eval(
        :(
            @everywhere using DaskDistributedDispatcher;
            @everywhere using Memento;
        )
    )
    for p in pnums
        cond = @spawnat p Memento.config(LOG_LEVEL; fmt="[{level} | {name}]: {msg}")
        wait(cond)
    end
    return pnums
end

##############################          TEST SERVER            #############################

type TestServer <: Server
    address::Address
    listener::Base.TCPServer
    handlers::Dict{String, Function}
end

function TestServer()
    handlers = Dict("ping" => ping)
    port, listener = listenany(rand(1024:9000))
    test_server = TestServer(Address(getipaddr(), port), listener, handlers)
    start_listening(test_server)
    return test_server
end

function ping(test_server::TestServer, comm::TCPSocket; delay::String="0.0")
    sleep(parse(delay))
    return "pong"
end

##############################          ECHO SERVER            #############################

type EchoServer <: Server
    address::Address
    listener::Base.TCPServer
end

function EchoServer()
    port, listener = listenany(rand(1024:9000))
    echo_server = EchoServer(Address(getipaddr(), port), listener)
    start_listening(echo_server, handler=handle_comm)
    return echo_server
end

function handle_comm(server::EchoServer, comm::TCPSocket)
    @async begin
        while isopen(comm)
            try
                msg = recv_msg(comm)
                send_msg(comm, msg)
            catch e
                # Errors are expected when connections are closed unexpectedly
                if !(isa(e, EOFError) || isa(e, Base.UVError) || isa(e, ArgumentError))
                    rethrow(e)
                end
            end
        end
    end
end

##############################            TESTS               ##############################


@testset "Communication" begin
    @testset "ConnectionPool" begin
        @testset "Basic ConnectionPool" begin
            servers = [TestServer() for i in 1:10]
            connection_pool = ConnectionPool(5)

            msg = Dict("op" => "ping")

            # Test reuse connections
            for s in servers[1:5]
                send_recv(connection_pool, s.address, msg)
            end
            for s in servers[1:5]
                send_recv(connection_pool, Address("127.0.0.1:$(s.address.port)"), msg)
            end

            @test sum(map(length, values(connection_pool.available))) == 5
            @test sum(map(length, values(connection_pool.occupied))) == 0
            @test connection_pool.num_active == 0
            @test connection_pool.num_open == 5

            # Clear out connections to make room for more
            for s in servers[6:end]
                send_recv(connection_pool, s.address, msg)
            end

            @test connection_pool.num_active == 0
            @test connection_pool.num_open == 5

            msg = Dict("op" => "ping", "delay" => 0.1)
            s = servers[1]
            @sync begin
                for i in 1:3
                    @async send_recv(connection_pool, s.address, msg)
                end
            end
            @assert length(connection_pool.available[s.address]) == 3

            close(connection_pool)
        end

        @testset "ConnectionPool with closing comms" begin
            global closed_available = false
            global closed_occupied = false

            function terminate_comms(pool::ConnectionPool)
                @async while true
                    sec = rand(1:5)/1000
                    sleep(sec)

                    if rand(1:10) > 5 && !closed_available
                        if !isempty(pool.available)
                            comms = collect(rand(collect(values(pool.available))))
                            if !isempty(comms)
                                close(rand(comms))
                            end

                        end
                    else
                        if !isempty(pool.occupied)
                            comms = collect(rand(collect(values(pool.occupied))))
                            if !isempty(comms)
                                close(rand(comms))
                                closed_occupied = true
                            end
                        end
                    end
                end
            end

            servers = [TestServer() for i in 1:10]
            connection_pool = ConnectionPool(10)
            terminate_comms(connection_pool)

            msg = Dict("op" => "ping")

            while !(closed_available || closed_occupied)
                for s in servers[1:10]
                    try
                        send_recv(connection_pool, s.address, msg)
                    catch e
                        # Errors are expected when connections are closed
                        isa(e, EOFError) || isa(e, ArgumentError) || rethrow(e)
                        closed_available = true
                    end
                end
            end
        end
    end

    @testset "BatchedSend" begin
        echo_server = EchoServer()

        @testset "Basic BatchedSend" begin
            comm = connect(echo_server.address)
            batchedsend = BatchedSend(comm, interval=0.01)
            sleep(0.02)

            send_msg(batchedsend, "hello")
            send_msg(batchedsend, "hello")
            send_msg(batchedsend, "world")

            sleep(0.02)
            send_msg(batchedsend, "HELLO")
            send_msg(batchedsend, "HELLO")

            result = recv_msg(comm)
            @test result == ["hello", "hello", "world"]
            result = recv_msg(comm)
            @test result == ["HELLO", "HELLO"]
            close(batchedsend)
        end

        @testset "BatchedSend sends messages before closing" begin
            comm = connect(echo_server.address)
            batchedsend = BatchedSend(comm, interval=0.01)

            send_msg(batchedsend, "hello")
            close(batchedsend)
        end
    end
end


@testset "Communication utils" begin
    @testset "Read messages" begin
        test_msg = [Dict{Any, Any}(
            UInt8[0x6f,0x70] =>
            UInt8[0x73,0x74,0x72,0x65,0x61,0x6d,0x2d,0x73,0x74,0x61,0x72,0x74]
        )]
        @test read_msg(test_msg) == [Dict{Any, Any}("op" => "stream-start")]
    end

    @testset "Serialization" begin
        op = Op(Int, 2.0)

        serialized_func = to_serialize(op.func)
        serialized_args = to_serialize(op.args)
        serialized_kwargs = to_serialize(op.kwargs)
        serialized_future = to_serialize(op.result)

        @test to_deserialize(serialized_func) == op.func
        @test to_deserialize(serialized_func) == op.func
        @test to_deserialize(serialized_func) == op.func
        @test to_deserialize(serialized_func) == op.func
    end

    @testset "Data packing" begin
        data = Dict("x" =>  1)
        @test pack_data(("x", "y"), data) == (1, "y")
        @test pack_data(["x", "y"], data) == [1, "y"]
        @test pack_data(Set(["x", "y"]), data) == Set([1, "y"])

        item = Dict("a" => "x")
        @test pack_data(item, data) == Dict("a" => 1)

        item = Dict("a" => "x", "b" => "y")
        @test pack_data(item, data) == Dict("a" => 1, "b" => "y")

        item = Dict("a" => ["x"], "b" => "y")
        @test pack_data(item, data) == Dict("a" => ["x"], "b" => "y")
    end

    @testset "Data unpacking" begin
        op = Op(Int, 2.0)
        op_key = get_key(op)

        @test unpack_data(1) == 1
        @test unpack_data(()) == ()
        @test unpack_data(op) == op_key
        @test unpack_data([1, op]) == [1, op_key]
        @test unpack_data(Dict(1 => op)) == Dict(1 => op_key)
        @test unpack_data(Dict(1 => [op])) == Dict(1 => [op])
    end
end


@testset "Addressing" begin
    @testset "Address parsing" begin
        @test parse_address("tcp://$host:51440") == ("tcp", host_ip, 51440)
        @test parse_address("tcp://127.0.0.1:51440") == ("tcp", host_ip, 51440)
        @test parse_address("$host:51440") == ("tcp", host_ip, 51440)
        @test parse_address("$host") == ("tcp", host_ip, 0)
        @test parse_address("$host:") == ("tcp", host_ip, 0)
        @test parse_address("51440") == ("tcp", ip"0.0.200.240", 0)

        @test_throws Exception parse_address(":51440")
    end

    @testset "Address building" begin
        @test string(Address("tcp://$host:51440")) == "tcp://$host:51440"
        @test string(Address("tcp://127.0.0.1:51440")) == "tcp://$host:51440"
        @test string(Address("$host:51440")) == "tcp://$host:51440"
        @test string(Address("$host")) == "tcp://$host:0"
        @test string(Address("$host:")) == "tcp://$host:0"
        @test string(Address("51440")) == "tcp://0.0.200.240:0"

        @test string(Address(host, 1024)) == "tcp://$host:1024"
        @test string(Address("127.0.0.1", 1024)) == "tcp://$host:1024"

        @test_throws Exception Address(":51440")
        @test_throws Exception Address("tcp://::51440")
    end
end


@testset "Client with single worker" begin
    # Test sending to scheduler upon startup
    op = Op(-, 1, 1)
    client = Client("tcp://$host:8786")
    submit(client, op)

    @test client.scheduler.address.host == host_ip
    @test client.scheduler.address.port == 8786

    pnums = test_addprocs(1)

    try
        worker_address = @fetchfrom pnums[1] begin
            worker = Worker("tcp://$host:8786")

            address_port = string(worker.address.port)
            @test sprint(show, worker) == (
                "<Worker: tcp://$host:$address_port, starting, stored: 0, running: 0," *
                " ready: 0, comm: 0, waiting: 0>"
            )
            @test string(worker.address) == "tcp://$host:$address_port"
            @test string(worker.scheduler_address) == "tcp://$host:8786"

            return worker.address
        end

        @test fetch(op) == 0

        op1 = Op(Int, 2.0)
        submit(client, op1)
        @test fetch(op1) == 2

        op2 = Op(Int, 2.0)
        submit(client, op2)
        @test isready(op2) == false
        @test fetch(op2) == 2

        @test gather(client, [op1, op2]) == [2, 2]

        op3 = Op(Int, 2.3)
        submit(client, op3)
        @test isa(fetch(op3)[1], InexactError)

        op4 = Op(+, 10, 1)
        submit(client, op4)
        @test fetch(op4) == 11

        # Test resubmission of same task to the same worker
        clientside = connect(worker_address)
        msg1 = Dict("reply"=>"false","op"=>"compute-stream")
        send_msg(clientside, msg1)
        msg2 = Dict(
            "op" => "compute-task",
            "key" => get_key(op4),
            "priority" => [4, 0],
            "close" => true,
        )
        send_msg(clientside, msg2)
        close(clientside)

        @test isa(gather(client, [op3])[1][1], InexactError)

        # Test dependent ops
        op5 = Op(+, 5, op1)
        submit(client, op5)

        op6 = Op(+, op1, op5);
        submit(client, op6)

        @test fetch(op5) == 7
        @test fetch(op6) == 9

        op7 = Op(sprint, show, "hello")
        submit(client, op7)
        @test fetch(op7) == "\"hello\""

        # Test cancelling ops
        op8 = Op(sleep, 1)
        op9 = Op(sleep, 2)
        submit(client, op8)
        submit(client, op9)

        cancel(client, [op8, op9])
        @test !haskey(client.nodes, get_key(op8))
        @test !haskey(client.nodes, get_key(op9))

        # Make sure ops aren't executed
        sleep(5)
        @test !isready(op8) && !isready(op9)

        # Test submitting submits all dependencies as well
        op9 = Op(()->3)
        op10 = Op(+, op9, 2)
        op11 = Op(+, op9, 3)
        op12 = Op(+, op10, op11);
        submit(client, op12)
        @test gather(client, [op9, op10, op11, op12]) == [3, 5, 6, 11]

        # Test IndexNode's execution
        node = IndexNode(Op(Int, 4.0), 1)
        submit(client, node)
        @test fetch(node) == 4

        # Test DataNode's execution
        node = DataNode(37)
        op13 = Op((x)->x, node)
        submit(client, op13)
        @test fetch(op13) == 37

        # Test terminating the client and workers
        shutdown([worker_address])
        shutdown(client)
        @test_throws ErrorException send_to_scheduler(client, Dict())
        @test_throws ErrorException shutdown(client)
        @test gather(client, [op]) == [0]
        @test_throws ErrorException submit(client, op)

    finally
        rmprocs(pnums; waitfor=1.0)
    end
end


@testset "Client with multiple workers" begin
    client = Client("tcp://$host:8786")

    pnums = test_addprocs(3)

    try
        worker1_address = @fetchfrom pnums[1] begin
            worker1 = Worker("tcp://$host:8786")
            return worker1.address
        end

        worker2_address = @fetchfrom pnums[2] begin
            worker2 = Worker("tcp://$host:8786")
            return worker2.address
        end

        worker3_address = @fetchfrom pnums[3] begin
            worker3 = Worker("tcp://$host:8786")
            return worker3.address
        end

        ops = Array{Op, 1}()
        push!(ops, Op(Int, 1.0))
        push!(ops, Op(Int, 2.0))
        push!(ops, Op(+, ops[1], ops[2]))
        push!(ops, Op(+, 1, ops[2], ops[3]))
        push!(ops, Op(Int, 5.0))
        push!(ops, Op(Int, 6.0))
        push!(ops, Op(sleep, 1.0))
        push!(ops, Op(+, 1, ops[6]))

        submit(client, ops[1], workers=[worker1_address])
        submit(client, ops[2], workers=[worker2_address])
        submit(client, ops[3], workers=[worker3_address])
        submit(client, ops[4], workers=[worker1_address])
        submit(client, ops[5], workers=[worker1_address])
        submit(client, ops[6], workers=[worker1_address])
        submit(client, ops[7])

        @test fetch(ops[1]) == 1
        @test fetch(ops[2]) == 2
        @test fetch(ops[3]) == 3
        @test fetch(ops[4]) == 6

        @test gather(client, ops[1:4]) == [1, 2, 3, 6]

        @test fetch(ops[5]) == 5
        @test fetch(ops[6]) == 6
        @test fetch(ops[7]) == nothing

        # Test gather
        keys_to_gather = [to_key(get_key(op)) for op in ops[1:7]]
        msg = Dict("op" => "gather", "keys" => keys_to_gather)
        comm = connect(8786)
        response = send_recv(comm, msg)

        @test response["status"] == "OK"
        results = Dict(k => to_deserialize(v) for (k,v) in response["data"])

        @test results[get_key(ops[1])] == 1
        @test results[get_key(ops[2])] == 2
        @test results[get_key(ops[3])] == 3
        @test results[get_key(ops[4])] == 6
        @test results[get_key(ops[5])] == 5
        @test results[get_key(ops[6])] == 6
        @test results[get_key(ops[7])] == nothing

        submit(client, ops[8], workers=[worker2_address])

        # Test worker execution crashing
        # Allow time for worker2 to deal with worker1 crashing
        rmprocs(pnums[1])
        sleep(15.0)

        # Test that worker2 was unable to complete op8
        @test !isready(ops[8])

        # Test shutting down the client and workers
        shutdown([worker2_address, worker3_address])
        shutdown(client)
        sleep(10)
    finally
        rmprocs(pnums[2:end]; waitfor=1.0)
    end
end

@testset "Multiple clients" begin
    client1 = Client("tcp://$host:8786")
    client2 = Client("tcp://$host:8786")
    client3 = Client("tcp://$host:8786")

    pnums = test_addprocs(1)

    try
        cond = @spawn Worker("tcp://$host:8786")
        wait(cond)

        @everywhere inc(x) = x + 1
        @everywhere dec(x) = x - 1
        @everywhere add(x, y) = x + y

        data1 = [1, 2, 3]
        data2 = [5, 6, 3]

        ops1 = [Op(add, Op(inc, x), Op(dec, x)) for x in data1]
        ops2 = [Op(add, Op(inc, x), Op(dec, x)) for x in data2]

        for op in ops1
            submit(client1, op)
        end

        for op in ops2
            submit(client2, op)
        end

        @test fetch(ops1[1]) == 2
        @test fetch(ops1[2]) == 4
        @test fetch(ops1[3]) == 6

        @test fetch(ops2[1]) == 10
        @test fetch(ops2[2]) == 12
        @test fetch(ops2[3]) == 6

        @test gather(client1, ops1) == [2, 4, 6]
        @test gather(client2, ops1) == [2, 4, 6]
        @test gather(client3, ops1) == [2, 4, 6]

        @test gather(client1, ops2) == [10, 12, 6]
        @test gather(client2, ops2) == [10, 12, 6]
        @test gather(client3, ops2) == [10, 12, 6]

        shutdown(client1)
        shutdown(client2)
        shutdown(client3)
    finally
        rmprocs(pnums; waitfor=1.0)
    end
end


@testset "Replication" begin
    client = Client("tcp://$host:8786")

    pnums = test_addprocs(3)

    function get_keys(worker_address::Address)
        clientside = connect(worker_address)
        response = send_recv(clientside, Dict("op" => "keys", "reply" => true))
        @test send_recv(clientside, Dict("op" => "close", "reply" => true)) == "OK"
        close(clientside)
        return response
    end

    try
        workers = Address[]
        for i in 1:5
             worker_address = @fetch begin
                worker = Worker("tcp://$host:8786")
                return worker.address
            end
            push!(workers, worker_address)
        end

        ops = Array{Op, 1}()
        push!(ops, Op(()->1))
        push!(ops, Op(()->2))
        push!(ops, Op(+, ops[1], ops[2]))
        push!(ops, Op(+, 1, ops[1], ops[2]))
        push!(ops, Op(()->5))
        push!(ops, Op(()->6))

        keys_replicated = Any[get_key(op) for op in ops]

        for i in 1:length(ops)
            submit(client, ops[i])
        end

        @test gather(client, ops) == [1,2,3,4,5,6]
        @test replicate(client) == "nothing"

        sleep(10)
        for worker_address in workers
            @test sort(get_keys(worker_address)) == sort(keys_replicated)
        end

        shutdown(workers)
        shutdown(client)
    finally
        rmprocs(pnums; waitfor=1.0)
    end
end


@testset "Dask - $i process" for i in 1:2
    pnums = i > 1 ? addprocs(i - 1) : ()
    @everywhere using DaskDistributedDispatcher

    comm = DeferredFutures.DeferredChannel()

    try
        ctx = DispatchContext()
        exec = DaskExecutor("$(getipaddr()):8786")

        workers = Address[]
        for i in 1:i
            worker_address = @fetch begin
                worker = Worker()
                return worker.address
            end
            push!(workers, worker_address)
        end

        op = Op(()->3)
        set_label!(op, "3")
        a = add!(ctx, op)

        op = Op((x)->x, 4)
        set_label!(op, "4")
        b = add!(ctx, op)

        op = Op(max, a, b)
        c = add!(ctx, op)

        op = Op(sqrt, c)
        d = add!(ctx, op)

        op = Op((x)->(factorial(x), factorial(2x)), c)
        set_label!(op, "factorials")
        e, f = add!(ctx, op)

        op = Op((x)->put!(comm, x / 2), f)
        set_label!(op, "put!")
        g = add!(ctx, op)

        result_truth = factorial(2 * (max(3, 4))) / 2

        results = run!(exec, ctx)

        @test fetch(unwrap(results[1])) == 2.0
        fetch(g)
        @test isready(comm)
        @test take!(comm) === result_truth
        @test !isready(comm)
        close(comm)

        shutdown(workers)
        shutdown(exec.client)
    finally
        rmprocs(pnums; waitfor=1.0)
    end
end


@testset "Dask - Application Errors" begin
    pnums = addprocs(1)
    @everywhere using DaskDistributedDispatcher

    comm = DeferredFutures.DeferredChannel()

    try
        ctx = DispatchContext()
        exec = DaskExecutor()

        cond = @spawn begin
            DaskDistributedDispatcher.Worker("tcp://$(getipaddr()):8786")
        end
        wait(cond)

        op = Op(()->3)
        set_label!(op, "3")
        a = add!(ctx, op)

        op = Op((x)->x, 4)
        set_label!(op, "4")
        b = add!(ctx, op)

        op = Op(max, a, b)
        c = add!(ctx, op)

        op = Op(sqrt, c)
        d = add!(ctx, op)

        error_op = Op(
            (x)-> (
                factorial(x),
                throw(ErrorException("Application Error"))
            ), c
        )
        set_label!(error_op, "ApplicationError")
        e, f = add!(ctx, error_op)

        op = Op((x)->put!(comm, x / 2), f)
        set_label!(op, "put!")
        g = add!(ctx, op)

        result_truth = factorial(2 * (max(3, 4))) / 2

        # Behaviour of `asyncmap` on exceptions changed
        # between julia 0.5 and 0.6
        if VERSION < v"0.6.0-"
            @test_throws CompositeException run!(exec, ctx)
        else
            @test_throws DependencyError run!(exec, ctx)
        end

        prepare!(exec, ctx)
        @test any(run!(exec, ctx; throw_error=false)) do result
            iserror(result) && isa(unwrap_error(result), DependencyError)
        end
        @test !isready(comm)
        close(comm)

        shutdown(exec.client)
    finally
        rmprocs(pnums; waitfor=1.0)
    end
end


@testset "Dask Do" begin
    worker = Worker()
    worker_address = worker.address

    function slowadd(x, y)
        return x + y
    end

    function slowinc(x)
        return x + 1
    end

    function slowsum(a...)
        return sum(a)
    end

    data = [1, 2, 3]

    ctx = @dispatch_context begin
        A = map(data) do i
            @op slowinc(i)
        end

        B = map(A) do a
            @op slowadd(a, 10)
        end

        C = map(A) do a
            @op slowadd(a, 100)
        end

        result = @op ((@op slowsum(A...)) + (@op slowsum(B...)) + (@op slowsum(C...)))
    end

    executor = DaskExecutor()
    (run_result,) = run!(executor, ctx, [result])

    @test !iserror(run_result)
    run_future = unwrap(run_result)
    @test isready(run_future)
    @test fetch(run_future) == 357

    # Test reset!
    reset!(executor)

    (run_result,) = run!(executor, ctx, [result])

    @test !iserror(run_result)
    run_future = unwrap(run_result)
    @test isready(run_future)
    @test fetch(run_future) == 357

    shutdown([worker_address])
    reset!(executor)
    sleep(10)
end


@testset "Dask Cluster" begin
    pnums = test_addprocs(3)

    @everywhere function load(address)
        sleep(rand() / 2)

        return 1
    end

    @everywhere function load_from_sql(address)
        sleep(rand() / 2)

        return 1
    end

    @everywhere function process(data, reference)
        sleep(rand() / 2)

        return 1
    end

    @everywhere function roll(a, b, c)
        sleep(rand() / 5)

        return 1
    end

    @everywhere function compare(a, b)
        sleep(rand() / 10)

        return 1
    end

    @everywhere function reduction(seq)
        sleep(rand() / 1)

        return 1
    end

    try
        workers = Address[]
        for i in 1:3
            worker_address = @fetchfrom pnums[i] begin
                worker = Worker()
                return worker.address
            end
            push!(workers, worker_address)
        end

        ctx = @dispatch_context begin
            filenames = ["mydata-$d.dat" for d in 1:100]
            data = [(@op load(filename)) for filename in filenames]

            reference = @op load_from_sql("sql://mytable")
            processed = [(@op process(d, reference)) for d in data]

            rolled = map(1:(length(processed) - 2)) do i
                a = processed[i]
                b = processed[i + 1]
                c = processed[i + 2]
                roll_result = @op roll(a, b, c)
                return roll_result
            end

            compared = map(1:200) do i
                a = rand(rolled)
                b = rand(rolled)
                compare_result = @op compare(a, b)
                return compare_result
            end

            best = @op reduction(@node CollectNode(compared))
        end

        executor = DaskExecutor()
        cond = @async (run_best,) = run!(executor, ctx, [best])

        timedwait(()->istaskdone(cond), 600.0)
        @test isready(best)

        isready(best) && notice(logger, "The best result is $(fetch(best)).")

        shutdown(workers)
        reset!(executor)
    finally
        rmprocs(pnums; waitfor=1.0)
    end
end

