using DaskDistributedDispatcher
using Base.Test
using Dispatcher
using Memento

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
    to_key

const LOG_LEVEL = "info"  # other options are "debug", "notice", "warn", etc.

Memento.config(LOG_LEVEL; fmt="[{level} | {name}]: {msg}")

const logger = get_logger(current_module())
const host_ip = getipaddr()
const host = string(host_ip)

inline_flag = Base.JLOptions().can_inline == 1 ? `` : `--inline=no`
cov_flag = ``
if Base.JLOptions().code_coverage == 1
    cov_flag = `--code-coverage=user`
elseif Base.JLOptions().code_coverage == 2
    cov_flag = `--code-coverage=all`
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
            connection_pool = ConnectionPool(limit=5)

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
            connection_pool = ConnectionPool(limit=10)
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

    @test_throws ErrorException default_client()

    # Test sending to scheduler upon startup
    op = Op(-, 1, 1)
    client = Client("tcp://$host:8786")
    submit(client, op)

    @test client.scheduler.address.host == host_ip
    @test client.scheduler.address.port == 8786

    # Test default client is set properly
    is_running() = client.status == "running"
    timedwait(is_running, 120.0)
    @test default_client() == client

    pnums = addprocs(
        1;
        exeflags=`$cov_flag $inline_flag --color=yes --check-bounds=yes --startup-file=no`
    )
    @everywhere using DaskDistributedDispatcher

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

        op1 = Op(Int, 2.0)

        @test_throws Exception result(client, op1)
        @test_throws Exception gather([op1])

        submit(client, op1)
        @test fetch(op1) == 2
        @test result(client, op1) == 2

        op2 = Op(Int, 2.0)
        submit(client, op2)

        # Test that a previously computed result will be re-used
        @test isready(op2) == false
        @test result(client, op2) == 2
        @test isready(op2) == true
        @test fetch(op2) == 2

        @test gather(client, [op1, op2]) == [2, 2]

        op3 = Op(Int, 2.3)
        submit(client, op3)
        @test_throws String result(client, op3)

        op4 = Op(+, 10, 1)
        submit(client, op4)
        @test result(client, op4) == 11

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

        @test gather(client, [op1, op2, op3, op4]) == [2,2,"error"=>"InexactError",11]

        # Test dependent ops
        op5 = Op(+, 5, op1)
        submit(client, op5)

        op6 = Op(+, op1, op5);
        submit(client, op6)

        @test result(client, op5) == 7
        @test result(client, op6) == 9

        # Test cancelling ops
        op7 = Op(sprint, show, "hello")
        submit(client, op7)
        cancel(client, [op7])
        @test_throws ErrorException result(client, op7)

        op8 = Op(*, 2, 2)
        submit(client, op8)

        op9 = Op(*, 3, 3)
        submit(client, op9)

        cancel(client, [op8, op9])
        @test !haskey(client.nodes, get_key(op8))
        @test !haskey(client.nodes, get_key(op9))

        @test_throws ErrorException gather(client, [op8, op9])

        # Test submitting submits all dependencies as well
        op9 = Op(()->3)
        op10 = Op(+, op9, 2)
        op11 = Op(+, op9, 3)
        op12 = Op(+, op10, op11);
        submit(client, op12)
        @test gather(client, [op9, op10, op11, op12]) == [3, 5, 6, 11]

        # Test terminating the client and workers
        shutdown([worker_address])
        shutdown(client)
        @test_throws ErrorException send_to_scheduler(client, Dict())
        @test_throws ErrorException shutdown(client)
        @test gather(client, [op]) == [0]
        @test_throws ErrorException submit(client, op)

    finally
        rmprocs(pnums)
    end
end


@testset "Client with multiple workers" begin
    client = Client("tcp://$host:8786")

    pnums = addprocs(
        3;
        exeflags=`$cov_flag $inline_flag --color=yes --check-bounds=yes --startup-file=no`
    )
    @everywhere using DaskDistributedDispatcher

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
        @test fetch(ops[1]) == 1
        @test result(client, ops[1]) == 1

        submit(client, ops[2], workers=[worker2_address])
        @test result(client, ops[2]) == 2

        submit(client, ops[3], workers=[worker3_address])
        @test result(client, ops[3]) == 3

        submit(client, ops[4], workers=[worker1_address])
        @test result(client, ops[4]) == 6

        @test gather(client, [ops[1], ops[2], ops[3], ops[4]]) == [1, 2, 3, 6]

        submit(client, ops[5], workers=[worker1_address])
        @test result(client, ops[5]) == 5

        submit(client, ops[6], workers=[worker1_address])
        @test result(client, ops[6]) == 6

        submit(client, ops[7])
        @test result(client, ops[7]) == nothing

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
        shutdown(client)
        sleep(5.0)
        shutdown([worker2_address, worker3_address])
        sleep(10.0)
    finally
        rmprocs(pnums[2:end])
    end
end


@testset "Dask Do" begin
    client = Client("tcp://$host:8786")

    worker = Worker("tcp://$host:8786")
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

    # TODO: use Dispatcher executor here once it has been implemented
    submit(client, result)

    @test fetch(result) == 357

    shutdown([worker_address])
    shutdown(client)
end


@testset "Dask Cluster" begin
    client = Client("tcp://$host:8786")

    pnums = addprocs(
        3;
        exeflags=`$cov_flag $inline_flag --color=yes --check-bounds=yes --startup-file=no`
    )
    @everywhere using DaskDistributedDispatcher

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
        worker1_address = @fetchfrom pnums[1] begin
            worker1 = Worker("tcp://$host:8786", validate=true)
            return worker1.address
        end

        worker2_address = @fetchfrom pnums[2] begin
            worker2 = Worker("tcp://$host:8786", validate=true)
            return worker2.address
        end

        worker3_address = @fetchfrom pnums[3] begin
            worker3 = Worker("tcp://$host:8786", validate=true)
            return worker3.address
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

        # TODO: use executor once it has been implemented
        submit(client, best)

        @test fetch(best) == true

        shutdown(client)
        shutdown([worker1_address, worker2_address, worker3_address])
    finally
        rmprocs(pnums)
    end
end

