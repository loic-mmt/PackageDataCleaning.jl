# load_raw_csv: thin, safe wrapper around csv.read

"""
load_raw_csv(path; delim=',', kwargs...) -> DataFrame

Charge un CSV brut dans un DataFrame.

- path: chemin vers le fichier CSV.
- delim: séparateur (par défaut "','").
- kwargs...: options passées à CSV.read (ex: 'ignorerepeated=true', 'missingstring=["","NA"]').

Lève un ArgumentError si le ficier n'existe pas.
"""

function read_raw_csv(path::AbstractString; delim = ',', kwargs...)
    isfile(path) || throw(ArgumentError("CSV file not found at: $path"))
    return CSV.read(path, DataFrame; delim = delim, kwargs)
end

@testset "read_raw_csv returns a DataFrame" begin
    df = DataFrame(
        a = ["1", "2", "3", "x", missing],
        b = ["chat", "chien", "chat", "souris", "chien"],
        c = ["", " ", "4", "5", "6"]
    )

    df2 = enforce_types(df)

    # Vérifie que a est numérique
    @test eltype(df2.a) <: Union{Missing, Int}

    # Vérifie que b est catégorielle
    @test isa(df2.b, CategoricalVector)

    # Vérifie que c est numérique (car majoritairement nombres)
    @test eltype(df2.c) <: Union{Missing, Float64}
end


# standardize_colnames: snake case, no special caracters, no spaces
"""
standardize_colnames()
"""