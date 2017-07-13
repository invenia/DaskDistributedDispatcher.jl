using DaskDistributedDispatcher
using Base.Test
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
    send_recv

const LOG_LEVEL = "debug"  # other options are "debug", "notice", "warn", etc.

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

##############################           TESTSERVER            #############################

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

function ping(test_server::TestServer, comm::TCPSocket; delay::AbstractFloat=0.0)
    sleep(delay)
    return "pong"
end

##############################            TESTS               ##############################


@testset "Client with single worker" begin

    @test_throws ErrorException default_client()

    # Test sending to scheduler upon startup
    op = Dispatcher.Op(-, 1, 1)
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

        op1 = Dispatcher.Op(Int, 2.0)

        @test_throws Exception result(client, op1)
        @test_throws Exception gather([op1])

        submit(client, op1)
        @test fetch(op1) == 2
        @test result(client, op1) == 2

        op2 = Dispatcher.Op(Int, 2.0)
        submit(client, op2)

        # Test that a previously computed result will be re-used
        @test isready(op2) == false
        @test result(client, op2) == 2
        @test isready(op2) == true
        @test fetch(op2) == 2

        @test gather(client, [op1, op2]) == [2, 2]

        op3 = Dispatcher.Op(Int, 2.3)
        submit(client, op3)
        @test_throws String result(client, op3)

        op4 = Dispatcher.Op(+, 10, 1)
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
        op5 = Dispatcher.Op(+, 5, op1)
        submit(client, op5)

        op6 = Dispatcher.Op(+, op1, op5);
        submit(client, op6)

        @test result(client, op5) == 7
        @test result(client, op6) == 9

        # Test cancels doesn't work right now
        op7 = Dispatcher.Op(sprint, show, "hello")
        submit(client, op7)
        @test_throws ErrorException cancel(client, [op7])
        @test result(client, op7) == "\"hello\""

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

        op1 = Dispatcher.Op(Int, 1.0)
        submit(client, op1, workers=[worker1_address])
        @test fetch(op1) == 1
        @test result(client, op1) == 1

        op2 = Dispatcher.Op(Int, 2.0)
        submit(client, op2, workers=[worker2_address])
        @test result(client, op2) == 2

        op3 = Dispatcher.Op(+, op1, op2)
        submit(client, op3, workers=[worker3_address])
        @test result(client, op3) == 3

        op4 = Dispatcher.Op(+, 1, op2, op3)
        submit(client, op4, workers=[worker1_address])
        @test result(client, op4) == 6

        @test gather(client, [op1, op2, op3, op4]) == [1, 2, 3, 6]

        op5 = Dispatcher.Op(Int, 5.0)
        submit(client, op5, workers=[worker1_address])
        @test result(client, op5) == 5

        op6 = Dispatcher.Op(Int, 6.0);
        submit(client, op6, workers=[worker1_address])
        @test result(client, op6) == 6

        op7 = Dispatcher.Op(sleep, 1.0);
        submit(client, op7)
        @test result(client, op7) == nothing

        op8 = Dispatcher.Op(+, 1, op6);
        submit(client, op8, workers=[worker2_address])

        # Test worker execution crashing
        # Allow time for worker2 to deal with worker1 crashing
        rmprocs(pnums[1])
        sleep(15.0)

        # Test that worker2 was unable to complete op8
        @test !isready(op8)

        # Test shutting down the client and workers
        shutdown(client)
        sleep(2.0)
        shutdown([worker2_address, worker3_address])
        sleep(10.0)

    finally
        rmprocs(pnums[2:end])
    end
end


@testset "Communication" begin
    @testset "ConnectionPool" begin
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
        op = Dispatcher.Op(Int, 2.0)

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
        op = Dispatcher.Op(Int, 2.0)
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
