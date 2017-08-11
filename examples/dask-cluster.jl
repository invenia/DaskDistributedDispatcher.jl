# based on http://matthewrocklin.com/blog/work/2017/01/24/dask-custom
const i = isempty(ARGS) ? 4 : parse(Int, ARGS[1])

addprocs(i)

using DaskDistributedDispatcher
using Dispatcher
using Memento

const LOG_LEVEL = "info"      # could also be "debug", "notice", "warn", etc

Memento.config(LOG_LEVEL; fmt="[{level} | {name}]: {msg}")
const logger = get_logger(current_module())

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


function main()
    workers = Address[]
    for i in 1:i
        worker_address = @fetch begin
            worker = Worker()
            return worker.address
        end
        push!(workers, worker_address)
    end

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

    best = @op reduction(CollectNode(compared))

    executor = DaskExecutor()
    (run_best,) = run!(executor, [best])

    shutdown(workers)
    shutdown(executor.client)

    return run_best
end

main()
@time main()
