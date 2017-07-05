using DaskDistributedDispatcher
using Base.Test
using DataStructures
using Memento
using URIParser

import DaskDistributedDispatcher:
    read_msg,
    parse_address,
    build_URI,
    to_serialize,
    to_deserialize,
    pack_data,
    unpack_data

const LOG_LEVEL = "debug"  # other options are "debug", "notice", "warn", etc.

Memento.config(LOG_LEVEL; fmt="[{level} | {name}]: {msg}")

const logger = get_logger(current_module())
const host = string(getipaddr())

inline_flag = Base.JLOptions().can_inline == 1 ? `` : `--inline=no`
cov_flag = ``
if Base.JLOptions().code_coverage == 1
    cov_flag = `--code-coverage=user`
elseif Base.JLOptions().code_coverage == 2
    cov_flag = `--code-coverage=all`
end

@testset "Client with single worker" begin
    client = Client("tcp://$host:8786")
    @test client.scheduler.address.host == "$host"
    @test client.scheduler.address.port == 8786

    pnums = addprocs(
        1;
        exeflags=`$cov_flag $inline_flag --color=yes --check-bounds=yes --startup-file=no`
    )
    @everywhere using DaskDistributedDispatcher

    try
        @fetchfrom pnums[1] begin
            worker = Worker("tcp://$host:8786")

            address_port = string(worker.port)
            @test sprint(show, worker) == (
                "<Worker: tcp://$host:$address_port, starting, stored: 0, running: 0," *
                " ready: 0, comm: 0, waiting: 0>"
            )

            @test string(worker.host) == "$host"
            @test worker.scheduler_address.host == "$host"
            @test worker.scheduler_address.port == 8786
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
        @test result(client, op3) == "InexactError"

        op4 = Dispatcher.Op(+, 10, 1)
        submit(client, op4)
        @test result(client, op4) == 11

        @test gather(client, [op1, op2, op3, op4]) == [2, 2, "InexactError", 11]

        # Test dependent ops
        op5 = Dispatcher.Op(+, 5, op1)
        submit(client, op5)

        op6 = Dispatcher.Op(+, op1, op5);
        submit(client, op6)

        @test result(client, op5) == 7
        @test result(client, op6) == 9

        shutdown(client)
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
            return address(worker1)
        end

        worker2_address = @fetchfrom pnums[2] begin
            worker2 = Worker("tcp://$host:8786")
            return address(worker2)
        end

        worker3_address = @fetchfrom pnums[3] begin
            worker3 = Worker("tcp://$host:8786")
            return address(worker3)
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

        op7 = Dispatcher.Op(sleep, 5.0);
        submit(client, op7)
        @test result(client, op7) == nothing

        shutdown(client)
    finally
        rmprocs(pnums)
    end
end


@testset "Communication" begin
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
    @testset "Parsing Addresses" begin
        @test parse_address("tcp://10.255.0.247:51440") == (ip"10.255.0.247", 51440, "tcp")
        @test parse_address("10.255.0.247:51440") == (ip"10.255.0.247", 51440, "tcp")
        @test parse_address("10.255.0.247") == (ip"10.255.0.247", 0, "tcp")
        @test parse_address("10.255.0.247:") == (ip"10.255.0.247", 0, "tcp")
        @test parse_address("51440") == (ip"0.0.200.240", 0, "tcp")

        @test_throws Exception parse_address(":51440")
    end

    @testset "Building URIs" begin
        @test build_URI("tcp://10.255.0.247:51440") == URI("tcp://10.255.0.247:51440")
        @test build_URI("10.255.0.247:51440") == URI("tcp://10.255.0.247:51440")
        @test build_URI("10.255.0.247") == URI("tcp://10.255.0.247")
        @test build_URI("10.255.0.247:") == URI("tcp://10.255.0.247")
        @test build_URI("51440") == URI("tcp://0.0.200.240")

        @test_throws Exception build_URI(":51440")
    end
end
