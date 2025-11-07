# load_raw_csv, validate_schema, standardize_colnames, enforce_types, deduplicate_rows


"""
    load_raw_csv(path; delim=',', kwargs...) -> DataFrame

Charge un CSV brut dans un DataFrame.

- path: chemin vers le fichier CSV.
- delim: séparateur (par défaut "','").
- kwargs...: options passées à CSV.read (ex: 'ignorerepeated=true', 'missingstring=["","NA"]').

Lève un ArgumentError si le fichier n'existe pas.
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

- `df` : DataFrame à contrôler.
- `required_columns` : collection de `Symbol` ou `String` représentant les noms attendus.
- `strict` :
    - `true`  -> lève un `ArgumentError` s'il manque des colonnes.
    - `false` -> renvoie le vecteur des colonnes manquantes.

Cette fonction illustre le multiple dispatch : le comportement réel est déterminé
par le type du troisième argument (`StrictMode` vs `LenientMode`), pas seulement
par une condition sur un booléen.
"""

abstract type SchemaMode end
struct StrictMode <: SchemaMode end
struct LenientMode <: SchemaMode end

"Calcule la liste des colonnes manquantes (interne)."
function _missing_columns(df::AbstractDataFrame, required_columns)
    req_syms = Symbol.(required_columns)
    present = Set(names(df))
    return [c for c in req_syms if !(c in present)]
end

"Mode strict : erreur si des colonnes manquent, sinon `true`."
function validate_schema(df::AbstractDataFrame, required_columns, ::StrictMode)
    missing = _missing_columns(df, required_columns)
    if isempty(missing)
        return true
    else
        missing_str = join(string.(missing), ", ")
        throw(ArgumentError("Missing required columns: $missing_str"))
    end
end

"Mode tolérant : renvoie la liste des colonnes manquantes (éventuellement vide)."
function validate_schema(df::AbstractDataFrame, required_columns, ::LenientMode)
    return _missing_columns(df, required_columns)
end


function validate_schema(df::AbstractDataFrame, required_columns; strict::Bool=true)
    if strict
        return validate_schema(df, required_columns, StrictMode())
    else
        return validate_schema(df, required_columns, LenientMode())
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

Cette deuxième méthode du même nom illustre le multiple dispatch : Julia choisit
automatiquement entre la version "un seul DataFrame" et la version "collection
de DataFrames" selon le type de l'argument.
"""
function standardize_colnames!(dfs::AbstractVector{<:AbstractDataFrame})
    for df in dfs
        standardize_colnames!(df)
    end
    return dfs
end



function enforce_types(df::DataFrame; num_threshold=0.9, max_factor_levels=20)
    out = copy(df)
    for col in names(out)
        x = out[!, col]

        if eltype(x) <: Union{Number, CategoricalValue}
            continue
        end

        xs = [ismissing(v) ? missing : strip(String(v)) for v in x]
        valid = filter(v -> !ismissing(v) && v != "", xs)
        n_valid = length(valid)
        if n_valid == 0
            continue
        end

        parsed = tryparse.(Float64, xs)
        n_numeric_valid = count(!ismissing, parsed)

        if n_numeric_valid / n_valid >= num_threshold
            nums = Float64.(coalesce.(parsed, NaN))
            if all(ismissing(v) || isinteger(v) for v in parsed)
                out[!, col] = convert(Vector{Union{Missing, Int}}, round.(Int, nums))
            else
                out[!, col] = convert(Vector{Union{Missing, Float64}}, nums)
            end
            continue
        end

        n_unique = length(unique(valid))
        if n_unique <= max_factor_levels
            out[!, col] = categorical(xs)
        else
            out[!, col] = xs
        end
    end
    return out
end