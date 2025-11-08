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



# deduplicate_rows

"""
    deduplicate_rows(df, mode::DedupMode; by=names(df),
                     blind_rows = Int[],
                     blind_col::Union{Symbol,Nothing} = nothing,
                     blind_values = nothing) -> DataFrame

Supprime ou conserve les doublons selon le mode choisi.

- `df` : DataFrame d'entrée (non muté).
- `mode` : stratégie typée :
    - `KeepFirst()` : conserve la première occurrence, supprime les doublons suivants.
    - `DropAll()`   : supprime **toutes** les lignes appartenant à un groupe dupliqué (ne garde que les lignes uniques).
- `by` : colonnes utilisées pour définir l'égalité entre lignes (par défaut toutes les colonnes).
- `blind_rows` : indices de lignes à ne jamais supprimer (protégées).
- `blind_col` / `blind_values` :
    - si spécifiés, toute ligne dont `df[!, blind_col]` est dans `blind_values` est protégée.
"""

abstract type DedupMode end
struct KeepFirst <: DedupMode end      # garde la première occurrence
struct DropAll   <: DedupMode end      # ne garde que les clés (valeurs) apparaissant une seule fois


# construit la clé de déduplication pour une ligne
@inline _dedup_key(df::AbstractDataFrame, i::Int, by) =
    ntuple(j -> df[i, by[j]], length(by))

# indique si une ligne est "protégée" (jamais supprimée)
function _is_protected(df::AbstractDataFrame, i::Int,
                       blind_rows::AbstractVector{Int},
                       blind_col::Union{Symbol,Nothing},
                       blind_values)
    if i in blind_rows
        return true
    end
    if blind_col !== nothing && blind_values !== nothing
        v = df[i, blind_col]
        return v in blind_values
    end
    return false
end

"Mode KeepFirst: conserve la 1ère occurrence, supprime les doublons suivants (sauf lignes protégées)."
function deduplicate_rows(df::AbstractDataFrame, ::KeepFirst;
                          by = names(df),
                          blind_rows::AbstractVector{Int} = Int[],
                          blind_col::Union{Symbol,Nothing} = nothing,
                          blind_values = nothing)

    by_syms = Symbol.(by)
    seen = Set{Tuple}()
    keep = trues(nrow(df))

    for i in 1:nrow(df)
        if _is_protected(df, i, blind_rows, blind_col, blind_values)
            # Protégée: on la garde, mais elle compte dans les "seen"
            key = _dedup_key(df, i, by_syms)
            push!(seen, key)
            continue
        end

        key = _dedup_key(df, i, by_syms)
        if key in seen
            keep[i] = false
        else
            push!(seen, key)
        end
    end

    return df[keep, :]
end

"Mode DropAll: ne garde que les lignes dont la clé n'apparaît qu'une seule fois (sauf lignes protégées)."
function deduplicate_rows(df::AbstractDataFrame, ::DropAll;
                          by = names(df),
                          blind_rows::AbstractVector{Int} = Int[],
                          blind_col::Union{Symbol,Nothing} = nothing,
                          blind_values = nothing)

    by_syms = Symbol.(by)

    # Compter le nombre d'occurrences de chaque clé
    counts = Dict{Tuple,Int}()
    for i in 1:nrow(df)
        key = _dedup_key(df, i, by_syms)
        counts[key] = get(counts, key, 0) + 1
    end

    keep = trues(nrow(df))
    for i in 1:nrow(df)
        if _is_protected(df, i, blind_rows, blind_col, blind_values)
            continue
        end
        key = _dedup_key(df, i, by_syms)
        if counts[key] > 1
            keep[i] = false
        end
    end

    return df[keep, :]
end