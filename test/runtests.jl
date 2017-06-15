using DaskDistributedDispatcher
using Base.Test
using Memento
using URIParser

# TODO: just use DaskDistributedDispatcher.read_msg instead of importing it?
import DaskDistributedDispatcher: read_msg, parse_address, build_URI

const LOG_LEVEL = "debug"  # other options are "debug", "notice", "warn", etc.

Memento.config(LOG_LEVEL; fmt="[{level} | {name}]: {msg}")
const logger = get_logger(current_module())
const host = string(getipaddr())

@testset "Worker" begin
    worker = Worker("tcp://$host:8786")

    @test isopen(worker.comm) == true
    @test worker.scheduler_address.host == "$host"
    @test worker.scheduler_address.port == 8786

    address_port = string(worker.port)

    @test string(worker.host) == "$host"

    @test sprint(show, worker) == (
        "<Worker: tcp://$host:$address_port/, starting, stored: 0, running: 0," *
        " ready: 0, comm: 0, waiting: 0>"
    )

    # Submit task
    clientside = connect(worker.port)

    msg = Dict(
        :key => "Int64-14699973390792368698",
        :duration => "0.5",
        :priority => ["7","0"],
        :func => "Int64",
        :args => ["2.0"],
    )

    task = Dict("task"=>nothing, "kwargs"=>nothing, "args"=>["2.0"], "func"=>"Int64")

    DaskDistributedDispatcher.add_task(worker, ;msg...)

    @test_throws ArgumentError DaskDistributedDispatcher.add_task(worker)


    # @test worker.tasks["Int64-14699973390792368698"] == task

end

@testset "Client with single worker" begin
    pnum = addprocs(1)
    @everywhere using DaskDistributedDispatcher

    try
        client = Client("tcp://$host:8786")
        @test client.scheduler.address.host == "$host"
        @test client.scheduler.address.port == 8786

        @spawn Worker("tcp://$host:8786")

        op = Dispatcher.Op(Int, 2.0)
        submit(client, op);
    finally
        rmprocs(pnum,  waitfor=2.0)
    end

#         # keyedfuture = submit(client, Int, 2.0)

#         # @test keyedfuture.key == "Int64-14699973390792368698"
#         # @test string(keyedfuture.state.future) == "Future(1,1,1,Nullable{Any}())"

#         # # Submit op with same key
#         # keyedfuture = submit(client, Int, 2.0)
#         # @test keyedfuture.key == "Int64-14699973390792368698"
#         # @test string(keyedfuture.state.future) == "Future(1,1,1,Nullable{Any}())"

#         keyedfuture = submit(client, +, 10, 1)

#         @test keyedfuture.key == "+-6630278134604469612"
#         @test string(keyedfuture.state.future) == "Future(1,1,2,Nullable{Any}())"

#         @test result(keyedfuture) == 11

end

#     @testset "Submit task to worker" begin
#         worker = Worker("10.255.0.247:8786")

#         clientside = connect(worker.port)

#         msg = Dict(
#             :key => "Int64-14699973390792368698",
#             :duration => "0.5",
#             :priority => ["7","0"],
#             :func => "Int64",
#             :args => ["2.0"],
#         )

#         task = Dict("task"=>nothing, "kwargs"=>nothing, "args"=>["2.0"], "func"=>"Int64")

#         DaskDistributedDispatcher.add_task(worker, ;msg...)

#         # @test worker.tasks["Int64-14699973390792368698"] == task
#     end

# end

@testset "Communication" begin
    test_msg = [Dict{Any, Any}(
        UInt8[0x6f,0x70] =>
        UInt8[0x73,0x74,0x72,0x65,0x61,0x6d,0x2d,0x73,0x74,0x61,0x72,0x74]
    )]
    @test read_msg(test_msg) == [Dict{Any, Any}("op" => "stream-start")]
end

@testset "Addressing" begin
    @testset "Parse Address" begin
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
