# load_raw_csv, validate_schema, standardize_colnames, enforce_types, deduplicate_rows


"""
    load_raw_csv(path::AbstractString; delim=',', kwargs...) -> DataFrame
    load_raw_csv(io::IO; delim=',', kwargs...) -> DataFrame

Charge un CSV brut dans un `DataFrame`.

Cette fonction propose deux variantes :

- `load_raw_csv(path::AbstractString; ...)` : lit un fichier CSV à partir d'un chemin sur le disque.
- `load_raw_csv(io::IO; ...)` : lit un CSV à partir d'un flux IO déjà ouvert (par ex. `IOBuffer`, fichier ouvert).

# Arguments

- `path` : chemin vers le fichier CSV.
- `io`   : flux IO déjà ouvert contenant des données CSV.
- `delim` : séparateur de colonnes (par défaut `','`).
- `kwargs...` : options passées à `CSV.read` (par ex. `ignorerepeated=true`, `missingstring=["","NA"]`).

# Exceptions

- Lève un `ArgumentError` si `path` ne correspond à aucun fichier existant.

# Exemples

Lecture classique depuis un fichier sur le disque :

```julia
df = load_raw_csv("data.csv")
```

Lecture depuis un texte CSV en mémoire :

```julia
text = \"\"\"
col1,col2
1,2
3,4
\"\"\"

buf = IOBuffer(text)
df2 = load_raw_csv(buf)
```
"""
function load_raw_csv end

function load_raw_csv(path::AbstractString; delim = ',', kwargs...)
    isfile(path) || throw(ArgumentError("CSV file not found at: $path"))
    return CSV.read(path, DataFrame; delim=delim, kwargs...)
end

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

Cette fonction utilise le multiple dispatch : le comportement réel est déterminé
par le type du troisième argument (`StrictMode` vs `LenientMode`), pas seulement
par une condition sur un booléen.

Exemple d'utiliation :

        df = DataFrame(a = [1], b = [2])
        validate_schema(df, [:a, :b, :c])
        # ArgumentError -> Missing required columns: c

        validate_schema(df, [:a, :b, :c]; strict=false)
        # missing c
"""

abstract type SchemaMode end
struct StrictMode <: SchemaMode end
struct LenientMode <: SchemaMode end

"Calcule la liste des colonnes manquantes (interne)."
function _missing_columns(df::AbstractDataFrame, required_columns)
    req_syms = Symbol.(required_columns)
    present = Set(Symbol.(names(df)))
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

Exemple d'utilisation : 

        df = DataFrame("  My Col (1) " => [1,2], "SALAIRE (€)" => [10,20])
        standardize_colnames!(df)
        names(df) 
        # "my_col_1", "salaire"
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

Cette deuxième méthode utilise le multiple dispatch : Julia choisit
automatiquement entre la version "un seul DataFrame" et la version "collection
de DataFrames" selon le type de l'argument.
"""
function standardize_colnames!(dfs::AbstractVector{<:AbstractDataFrame})
    for df in dfs
        standardize_colnames!(df)
    end
    return dfs
end

"""

Exemple d'utilisation :
df = DataFrame(
        a = ["1", "2", "3", "x", missing],
        b = ["chat", "chien", "chat", "souris", "chien"],
        c = ["", " ", "4", "5", "6"]
)
df2 = enforce_types(df)

isa(df2.a, CategoricalVector)
#True
isa(df2.b, CategoricalVector)
#True
eltype(df2.c)  <: Union{Missing, Int, Float64}
#True
"""

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

        parsed = map(xs) do v
            if ismissing(v) || v == ""
                missing
            else
                p = tryparse(Float64, String(v))
                p === nothing ? missing : p
            end
        end

        n_numeric_valid = count(!ismissing, parsed)

        if n_numeric_valid / n_valid >= num_threshold
            if all(ismissing(v) || isinteger(v) for v in parsed)
                # Colonne essentiellement entière (avec éventuellement des missings)
                out[!, col] = [ismissing(v) ? missing : round(Int, v) for v in parsed]
            else
                # Colonne numérique générale (Float64, avec éventuellement des missings)
                out[!, col] = [ismissing(v) ? missing : Float64(v) for v in parsed]
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

Exemples d'utilisation : 
df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
               b = ["a", "b", "b", "c", "d", "d", "e"])

out = deduplicate_rows(df, DropAll(); by = [:a]) # On déduplique par la colonne :a uniquement
size(out)
# 2, 2


df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
               b = ["a", "b", "b", "c", "d", "d", "e"])

out = deduplicate_rows(df, KeepFirst(); by = [:a])

size(out)
# 4, 2


df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
               b = ["a", "b", "b", "c", "d", "d", "e"])

out = deduplicate_rows(df, DropAll(); by = [:a], blind_rows = [1])
sort(out.a)
# 2, 3, 3, 3, 4
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
            # Protégée: on la garde mais elle compte dans les seen
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