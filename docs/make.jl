using Documenter, DaskDistributedDispatcher

makedocs(
    # options
    modules = [DaskDistributedDispatcher],
    format = :html,
    pages = [
        "Home" => "index.md",
        "Manual" => "pages/manual.md",
        "API" => "pages/api.md",
        "Workers" => "pages/workers.md",
        "Communication" => "pages/communication.md",
    ],
    sitename = "DaskDistributedDispatcher.jl",
    authors = "Invenia Technical Computing",
    assets = ["assets/invenia.css"],
)

deploydocs(
    repo = "github.com/invenia/DaskDistributedDispatcher.jl.git",
    julia = "0.5",
    target = "build",
    deps = nothing,
    make = nothing,
)
