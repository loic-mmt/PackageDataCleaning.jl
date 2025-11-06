# load_raw_csv, validate_schema, standardize_colnames, enforce_types, deduplicate_rows


"""
load_raw_csv(path; delim=',', kwargs...) -> DataFrame

Charge un CSV brut dans un DataFrame.

- path: chemin vers le fichier CSV.
- delim: séparateur (par défaut "','").
- kwargs...: options passées à CSV.read (ex: 'ignorerepeated=true', 'missingstring=["","NA"]').

Lève un ArgumentError si le ficier n'existe pas.
"""

function load_raw_csv(path::AbstractString; delim = ',', kwargs...)
    isfile(path) || throw(ArgumentError("CSV file not found at: $path"))
    return CSV.read(path, DataFrame; delim=delim, kwargs...)
end

"""

    load_raw_csv(io::IO; delim=',', kwargs...) -> DataFrame

Variante pour lire depuis un flux IO déjà ouvert (ex: `IOBuffer`, fichier ouvert).
Utilise la même logique que `load_raw_csv(path::AbstractString, ...)`.
"""
function load_raw_csv(io::IO; delim=',', kwargs...)
    return CSV.read(io, DataFrame; delim=delim, kwargs...)
end



"""
    validate_schema(df, required_columns; strict=true)

Vérifie que toutes les colonnes requises sont présentes dans le DataFrame.

- df : DataFrame à contrôler.
- required_columns : vecteur de Symbol ou de String représentant les noms attendus.
- strict :
    - true (défaut)  -> lève un ArgumentError s'il manque des colonnes.
    - false          -> renvoie le vecteur des colonnes manquantes (ou Symbol[] si OK).

Retourne `true` si le schéma est valide en mode strict.
"""
function validate_schema(df::AbstractDataFrame, required_columns; strict::Bool=true)
    req_syms = Symbol.(required_columns)
    present = Set(names(df))
    missing = [c for c in req_syms if !(c in present)]

    if isempty(missing)
        return true
    elseif strict
        missing_str = join(string.(missing), ", ")
        throw(ArgumentError("Missing required columns: $missing_str"))
    else
        return missing
    end
end
validate_schema(df::AbstractDataFrame, required_columns::Tuple; strict::Bool=true) =
    validate_schema(df, collect(required_columns); strict=strict)



"""
    standardize_colnames!(df)

Transforme les noms de colonnes en snake_case :
- minuscules
- caractères non alphanumériques remplacés par `_`
- underscores multiples réduits à un seul
- underscores en début/fin supprimés
"""
function standardize_colnames!(df)
    old = names(df)
    new = Symbol[]
    for n in old
        s = String(n)
        s = lowercase(s)
        s = replace(s, r"[^\p{L}\p{N}]+" => "_")
        s = replace(s, r"_+" => "_")
        s = strip(s, '_')
        push!(new, Symbol(s))
    end
    rename!(df, Pair.(old, new))
    return df
end

"""

    standardize_colnames!(dfs::AbstractVector{<:AbstractDataFrame})

Applique `standardize_colnames!` à chaque DataFrame d'une collection.
"""
function standardize_colnames!(dfs::AbstractVector{<:AbstractDataFrame})
    for df in dfs
        standardize_colnames!(df)
    end
    return dfs
end
