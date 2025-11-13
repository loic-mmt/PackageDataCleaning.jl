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
    _missing_columns(df::AbstractDataFrame, required_columns) -> Vector{Symbol}

Calcule la liste des colonnes manquantes dans un `DataFrame` par rapport à un
ensemble de colonnes requises.

Cette fonction est interne et utilisée par `validate_schema` pour factoriser
la logique de détection des colonnes manquantes.

# Arguments

- `df` : `AbstractDataFrame` dont on veut vérifier la présence des colonnes.
- `required_columns` : collection de noms de colonnes attendues (`Vector{Symbol}`,
  `Vector{String}`, `Tuple`, etc.). Tous les noms sont convertis en `Symbol`
  en interne.

# Retour

- Un `Vector{Symbol}` contenant les noms des colonnes requises qui ne sont pas
  présentes dans `df`. Le vecteur est vide si toutes les colonnes sont présentes.
"""
function _missing_columns(df::AbstractDataFrame, required_columns)
    req_syms = Symbol.(required_columns)
    present = Set(Symbol.(names(df)))
    return [c for c in req_syms if !(c in present)]
end


"""
    validate_schema(df::AbstractDataFrame, required_columns; strict=true)
    validate_schema(df::AbstractDataFrame, required_columns, ::StrictMode)
    validate_schema(df::AbstractDataFrame, required_columns, ::LenientMode)

Vérifie que toutes les colonnes requises sont présentes dans le `DataFrame`.

Cette fonction supporte deux modes de validation :

- `StrictMode` (par défaut via `strict=true`) : lève une erreur si des colonnes manquent.
- `LenientMode` (via `strict=false` ou appel explicite) : renvoie la liste des colonnes manquantes.

# Arguments

- `df` : `DataFrame` à contrôler.
- `required_columns` : collection de noms de colonnes attendues (`Vector{Symbol}`, `Vector{String}`, `Tuple`, etc.).
- `strict` :
    - `true`  : utilise `StrictMode()` et lève un `ArgumentError` s'il manque des colonnes.
    - `false` : utilise `LenientMode()` et renvoie un `Vector{Symbol}` des colonnes manquantes.

# Retour

- En mode strict (`strict=true` ou `StrictMode()`):
    - renvoie `true` si toutes les colonnes sont présentes ;
    - lève un `ArgumentError` sinon.
- En mode tolérant (`strict=false` ou `LenientMode()`):
    - renvoie un `Vector{Symbol}` (éventuellement vide) contenant les colonnes manquantes.

# Notes

- Les noms de colonnes sont normalisés en `Symbol` en interne.
- Une surchage `validate_schema(df, required_columns::Tuple; strict=true)` est fournie pour plus de confort : le tuple est simplement converti en vecteur.

# Exemples

Mode strict (erreur si colonnes manquantes) :

```julia
df = DataFrame(a = [1], b = [2])

validate_schema(df, [:a, :b]; strict=true)
# true

validate_schema(df, [:a, :b, :c]; strict=true)
# ArgumentError: Missing required columns: c

df = DataFrame(a = [1], b = [2])

missing_cols = validate_schema(df, [:a, :b, :c]; strict=false)
# [:c]

```
See also [`_missing_columns`](@ref)

"""
function validate_schema end

abstract type SchemaMode end
struct StrictMode <: SchemaMode end
struct LenientMode <: SchemaMode end

function validate_schema(df::AbstractDataFrame, required_columns, ::StrictMode)
    missing = _missing_columns(df, required_columns)
    if isempty(missing)
        return true
    else
        missing_str = join(string.(missing), ", ")
        throw(ArgumentError("Missing required columns: $missing_str"))
    end
end

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
    standardize_colnames!(df::AbstractDataFrame)
    standardize_colnames!(dfs::AbstractVector{<:AbstractDataFrame})

Normalise les noms de colonnes en `snake_case` pour un ou plusieurs `DataFrame`.

Les transformations appliquées à chaque nom de colonne sont :

- passage en minuscules ;
- remplacement des caractères non alphanumériques par `_` ;
- réduction des underscores multiples à un seul ;
- suppression des underscores en début et fin.

# Arguments

- `df`  : un `AbstractDataFrame` dont on veut standardiser les noms de colonnes.
- `dfs` : un vecteur de `AbstractDataFrame` ; la transformation est appliquée à chacun.

# Retour

- En entrée unique : renvoie le `DataFrame` modifié (les noms de colonnes sont mis à jour **en place**).
- En entrée collection : renvoie la collection `dfs`, après modification en place de chaque élément.

# Notes

- La fonction est mutante (`!`) : les noms de colonnes sont modifiés directement dans les `DataFrame`.
- Les nouveaux noms sont renvoyés sous forme de `Symbol`.
- Utile à combiner avec des fonctions comme [`enforce_types`] pour travailler sur des schémas de données plus propres.

# Exemples

Standardisation des colonnes d'un seul `DataFrame` :

```julia
df = DataFrame("  My Col (1) " => [1, 2], "SALAIRE (€)" => [10, 20])

standardize_colnames!(df)
names(df)
# Symbol[:my_col_1, :salaire]
df1 = DataFrame("My Col" => [1, 2])
df2 = DataFrame("Other Col" => [3, 4])

dfs = [df1, df2]
standardize_colnames!(dfs)

names(df1)  # [:my_col]
names(df2)  # [:other_col]
```
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

function standardize_colnames!(dfs::AbstractVector{<:AbstractDataFrame})
    for df in dfs
        standardize_colnames!(df)
    end
    return dfs
end





"""
    enforce_types(df::DataFrame; num_threshold=0.9, max_factor_levels=20) -> DataFrame

Tente d'inférer et d'appliquer des types plus adaptés pour les colonnes d'un `DataFrame`
à partir de valeurs textuelles ou mixtes.

Pour chaque colonne :

- si le type est déjà numérique ou catégoriel (`Number`, `CategoricalValue`), elle est laissée telle quelle ;
- sinon, la fonction essaie d'abord de la convertir en colonne numérique ;
- si la colonne n'est pas suffisamment "numérique", elle peut être convertie en facteur (`CategoricalVector`)
  si le nombre de modalités est raisonnable ;
- dans tous les autres cas, la colonne est nettoyée en une colonne de chaînes (`String`) avec `missing`.

# Arguments

- `df` : `DataFrame` d'entrée. Il n'est **pas** modifié ; une copie est retournée.
- `num_threshold` (par défaut `0.9`) :
    - fraction minimale de valeurs non vides pouvant être converties en nombre (`Float64`)
      pour considérer la colonne comme numérique ;
- `max_factor_levels` (par défaut `20`) :
    - nombre maximal de valeurs distinctes non vides pour convertir une colonne non numérique
      en `CategoricalVector`.

# Retour

- Un **nouveau** `DataFrame` dont les colonnes ont été éventuellement converties :
    - colonnes numériques : `Union{Missing, Int}` ou `Union{Missing, Float64}` ;
    - colonnes catégorielles : `CategoricalVector{String}` (avec `missing` possible) ;
    - autres colonnes : `Vector{Union{Missing, String}}` nettoyé (trim + chaînes vides traitées).

# Détails de l'inférence

- Les valeurs `missing` ou les chaînes vides (`""`, `" "`, etc.) sont ignorées dans les statistiques
  (ratio numérique, nombre de modalités).
- Si la proportion de valeurs convertibles en `Float64` parmi les valeurs non vides est
  ≥ `num_threshold` :
    - si toutes les valeurs numériques sont entières, la colonne est transformée en entiers
      (`Union{Missing, Int}`) ;
    - sinon, en `Union{Missing, Float64}`.
- Si la colonne n'est pas majoritairement numérique, mais que le nombre de modalités non vides
  est ≤ `max_factor_levels`, elle est convertie en `CategoricalVector`.
- Sinon, elle est conservée comme colonne de chaînes nettoyées.

# Exemples

Inférence de types sur des colonnes textuelles :

```julia
df = DataFrame(
    a = ["1", "2", "3", "x", missing],
    b = ["chat", "chien", "chat", "souris", "chien"],
    c = ["", " ", "4", "5", "6"]
)

df2 = enforce_types(df)

isa(df2.a, CategoricalVector)
# true (colonne peu numérique + peu de modalités)

isa(df2.b, CategoricalVector)
# true (texte avec peu de modalités)

eltype(df2.c) <: Union{Missing, Int, Float64}
# true (colonne majoritairement numérique)
```
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





"""
    _dedup_key(df::AbstractDataFrame, i::Int, by) -> Tuple

Construit la clé de déduplication pour la ligne `i` d'un `DataFrame` en
utilisant les colonnes listées dans `by`.

Cette fonction est interne et utilisée par `deduplicate_rows` pour identifier
les groupes de lignes dupliquées.

# Arguments

- `df` : `AbstractDataFrame` d'entrée.
- `i`  : indice de ligne (1-based).
- `by` : collection de noms de colonnes (souvent un vecteur de `Symbol`).

# Retour

- Un `Tuple` contenant les valeurs de la ligne `i` pour chaque colonne de `by`.
"""
@inline _dedup_key(df::AbstractDataFrame, i::Int, by) =
    ntuple(j -> df[i, by[j]], length(by))



    

"""
    _is_protected(df::AbstractDataFrame, i::Int,
                  blind_rows::AbstractVector{Int},
                  blind_col::Union{Symbol,Nothing},
                  blind_values) -> Bool

Indique si la ligne `i` d'un `DataFrame` est "protégée" contre la suppression
dans le cadre de `deduplicate_rows`.

Une ligne est considérée comme protégée si :

- son indice `i` appartient à `blind_rows`, ou
- `blind_col` et `blind_values` sont fournis et `df[i, blind_col] ∈ blind_values`.

# Arguments

- `df`          : `AbstractDataFrame` d'entrée.
- `i`           : indice de ligne (1-based).
- `blind_rows`  : vecteur d’indices de lignes à ne jamais supprimer.
- `blind_col`   : nom de colonne utilisé pour définir des lignes protégées (ou `nothing`).
- `blind_values`: collection de valeurs déclenchant la protection (ou `nothing`).

# Retour

- `true` si la ligne `i` doit être considérée comme protégée, `false` sinon.
"""
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




"""
    deduplicate_rows(df::AbstractDataFrame, mode::DedupMode;
                     by = names(df),
                     blind_rows::AbstractVector{Int} = Int[],
                     blind_col::Union{Symbol,Nothing} = nothing,
                     blind_values = nothing) -> DataFrame

    deduplicate_rows(df::AbstractDataFrame, ::KeepFirst; kwargs...) -> DataFrame
    deduplicate_rows(df::AbstractDataFrame, ::DropAll;   kwargs...) -> DataFrame

Supprime des lignes dupliquées dans un `DataFrame` selon une stratégie typée
(`DedupMode`) et des règles de protection optionnelles.

Deux modes sont proposés :

- `KeepFirst()` : conserve la **première** occurrence de chaque clé, supprime les doublons suivants ;
- `DropAll()`   : supprime **toutes** les lignes appartenant à une clé dupliquée (ne garde que les clés uniques).

# Arguments

- `df` : `AbstractDataFrame` d'entrée. Il n'est **pas** modifié ; une copie filtrée est retournée.
- `mode` : instance de `DedupMode` :
    - `KeepFirst()` ou
    - `DropAll()`.
- `by` :
    - collection de noms de colonnes (`Vector{Symbol}`, `Vector{String}`, etc.) définissant la *clé* de déduplication ;
    - par défaut `names(df)` (toutes les colonnes).
- `blind_rows` :
    - vecteur d’indices de lignes à **protéger** (elles ne sont jamais supprimées) ;
    - ces lignes comptent néanmoins dans les clés déjà vues.
- `blind_col` / `blind_values` :
    - si `blind_col` est un `Symbol` et `blind_values` une collection de valeurs,
      toute ligne telle que `df[i, blind_col] ∈ blind_values` est protégée ;
    - protection prioritaire : une ligne protégée n’est jamais supprimée, même si sa clé est dupliquée.

# Retour

- Un nouveau `DataFrame` contenant uniquement les lignes conservées selon :
    - la clé définie par `by` ;
    - le mode (`KeepFirst` / `DropAll`) ;
    - les règles de protection (`blind_rows`, `blind_col`, `blind_values`).

# Notes

- La fonction ne modifie pas `df` : elle construit un masque logique `keep` et renvoie `df[keep, :]`.
- Les lignes protégées (`blind_rows` / `blind_col` + `blind_values`) sont toujours présentes dans le résultat,
  même si leur clé apparaît plusieurs fois.
- En mode `DropAll`, les clés ayant plus d’une occurrence sont supprimées **sauf** pour les lignes protégées.

# Exemples

Déduplication en supprimant tous les groupes dupliqués :

```julia
df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
               b = ["a", "b", "b", "c", "d", "d", "e"])

out = deduplicate_rows(df, DropAll(); by = [:a])  # On déduplique uniquement par :a
size(out)
# (2, 2)  # seules les lignes avec a == 2 ou 4 sont conservées

df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
               b = ["a", "b", "b", "c", "d", "d", "e"])

out = deduplicate_rows(df, KeepFirst(); by = [:a])
size(out)
# (4, 2)  # a == 1, 2, 3, 4 (1ère occurrence de chaque clé)

df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
               b = ["a", "b", "b", "c", "d", "d", "e"])

out = deduplicate_rows(df, DropAll(); by = [:a], blind_rows = [1])
sort(out.a)
# [2, 3, 3, 3, 4]  # la ligne 1 (a == 1) est protégée, les autres clés uniques sont conservées
```

"""
function deduplicate_rows end
abstract type DedupMode end
struct KeepFirst <: DedupMode end      # garde la première occurrence
struct DropAll   <: DedupMode end      # ne garde que les clés (valeurs) apparaissant une seule fois
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