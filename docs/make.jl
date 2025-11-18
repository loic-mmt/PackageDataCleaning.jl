using PackageDataCleaning
using Documenter

DocMeta.setdocmeta!(PackageDataCleaning, :DocTestSetup, :(using PackageDataCleaning); recursive=true)

makedocs(;
    modules=[PackageDataCleaning],
    authors="Loïc Mémeteau <loic.memeteau@kedgebs.com>",
    sitename="PackageDataCleaning.jl",
    format=Documenter.HTML(;
        canonical="https://loic-mmt.github.io/PackageDataCleaning.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
        "API"  => "api.md",
    ],
    checkdocs = :none,
)

deploydocs(;
    repo="github.com/loic-mmt/PackageDataCleaning.jl",
    devbranch="main",
)
