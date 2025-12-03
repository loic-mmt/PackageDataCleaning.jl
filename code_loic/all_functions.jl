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

# validate_ranges, cap_outliers_salary

"""
A function for validation of conditions in variables

validate_range(data::SalaryTbl) -> DataFrame
Verify that in all the variables that can be verfied values are plausibles.
- 'data': le SalaryTbl à tester
- 'df': dataframe contenant le résulat du test (Booléen), pour chaque variable.
Permet de voir quelle variable contient l'erreur si il y en a une.

validate_range(data::DataFrame, vars_a_tester::AbstractVector, tests_a_effectuer::AbstractVector{Function}) -> DataFrame
Verify that in all the variables that can be verfied values are plausibles.
- 'data': le DataFrame à tester
- 'vars_a_tester': vecteur des variables à tester
- 'tests_a_effectuer': vecteur des tests à appliquer aux variables (fonctions qui renvoient un booléen)
- 'df': dataframe contenant le résulat du test (Booléen), pour chaque variable (char).
Permet de voir quelle variable contient l'erreur si il y en a une.

validate_range(var::AbstractVector, test::AbstractVector{Function})-> DataFrame
Verify that in all the variables that can be verfied values are plausibles.
- 'var': vecteur à tester
- 'test': fonction qui renvoie un booléen pour tester le vecteur
- 'verif': booléen value which indicate is the test as passed.
Permet de voir quelle variable contient l'erreur si il y en a une.
"""
function validate_range end


function validate_range(data::SalaryTbl)
    valid_mask = Bool[]
    push!(valid_mask, all(x -> x in EMPLOYMENT_TYPES, skipmissing(data[!, employment_type])))
    push!(valid_mask, all(x -> x in EXPERIENCE, skipmissing(data[!, experience_level])))
    push!(valid_mask, all(x -> x >0, skipmissing(data[!, salary])))
    push!(valid_mask, all(x -> x >0, skipmissing(data[!, salary_in_usd])))
    push!(valid_mask, all(x -> 0 <= x && x <= 100, skipmissing(data[!, remote_ratio])))
    push!(valid_mask, all(x -> x in SIZE, skipmissing(data[!, company_size])))
    df = DataFrame(variables = ["employment_type", "experience_level", "salary", "salary_in_usd", "remote_ratio", "company_size"], valid_mask = valid_mask)
    return df
end



function validate_range(data::DataFrame, vars_a_tester::AbstractVector, tests_a_effectuer::AbstractVector{Function})
    valid_mask = Bool[]
    for index in range length(vars_a_tester)
        push!(valid_mask, all(tests_a_effectuer[index], skipmissing(data[!, vars_a_tester[index]])))
    end
    df = DataFrame(variables = vars_a_tester, valid_mask = valid_mask)
    return df
end


function validate_range(var::AbstractVector, test::Function)
    verif = [var, all(test, skipmissing(var))]
    return verif
end



function winsorize end

function winsorize(vect::AbstractVector; lower_quantile=0.05, upper_quantile=0.95)
    lower = quantile(vect, lower_quantile)
    upper = quantile(vect, upper_quantile)
    return max(min(vect, upper), lower)
end

function winsorize(data::AbstractDataFrame; lower_quantile=0.05, upper_quantile=0.95)
    for col_name in names(data)
        col = data[!, col_name]
        if eltype(col) <: Real && length(col) > 1
            lower = quantile(col, lower_quantile)
            upper = quantile(col, upper_quantile)
            data[!, col_name] = max(min(col, upper), lower)
        end
    end
    return(data)
end

# normalize_experience_level, normalize_employment_type,
# normalize_company_size, normalize_remote_ratio,
# normalize_job_title, normalize_country_codes



"""
    NormalMode

Type abstrait pour représenter un mode d’ordonnancement dans certaines
normalisations (par ex. `CompanySize()`).
"""
abstract type NormalMode end

"""
    UptoDown <: NormalMode

Mode d’ordonnancement "du plus petit au plus grand" (ex: `S < M < L`).
Utilisé avec `CompanySize()` dans `normalize!`.
"""
struct UptoDown  <: NormalMode end

"""
    DowntoUp <: NormalMode

Mode d’ordonnancement "du plus grand au plus petit" (ex: `L < M < S`).
Utilisé avec `CompanySize()` dans `normalize!`.
"""
struct DowntoUp  <: NormalMode end

"""
    NormalizeField

Type abstrait pour représenter un "champ métier" normalisable
(type de contrat, taille d’entreprise, pays, etc.).
"""
abstract type NormalizeField end

"""
    EmploymentType <: NormalizeField

Champ métier pour normaliser une colonne de type de contrat
(ex: codes `FT`, `PT`, `CT`, `FL`).
"""
struct EmploymentType <: NormalizeField end

"""
    CompanySize <: NormalizeField

Champ métier pour normaliser une colonne de taille d’entreprise
(ex: `"S"`, `"M"`, `"L"`).
"""
struct CompanySize    <: NormalizeField end

"""
    RemoteRatio <: NormalizeField

Champ métier pour normaliser un ratio de télétravail en valeurs discrètes
(ex: `0`, `50`, `100`).
"""
struct RemoteRatio    <: NormalizeField end

"""
    JobTitle <: NormalizeField

Champ métier pour harmoniser les intitulés de poste via un dictionnaire
de correspondance.
"""
struct JobTitle       <: NormalizeField end

"""
    CountryCode <: NormalizeField

Champ métier pour convertir des pays / codes bruts en codes ISO2 cohérents
(et éventuellement une région).
"""
struct CountryCode    <: NormalizeField end


"""
    normalize!(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...)
    normalize!(df, EmploymentType(); col = :employment_type)
    normalize!(df, CompanySize(), UptoDown(); col = :company_size)
    normalize!(df, CompanySize(), DowntoUp(); col = :company_size)
    normalize!(df, RemoteRatio(); col = :remote_ratio, allowed = (0, 50, 100))
    normalize!(df, JobTitle(); col = :job_title, mapping = JOB_TITLE_MAPPING)
    normalize!(df, CountryCode(); col = :country,
               mapping = COUNTRY_CODE_MAPPING,
               region_col::Union{Symbol,Nothing} = nothing)

Applique une opération de normalisation en place sur `df` en fonction du type
de champ métier (`field`) et, éventuellement, d'un mode supplémentaire.

Cette fonction est le point d’entrée générique : pour chaque combinaison de `field`
et d’arguments (`args...` / `kwargs...`), une méthode spécialisée de `normalize!`
est sélectionnée par multiple dispatch.

# Variantes supportées

- `normalize!(df, EmploymentType(); col = :employment_type)`  
  Normalise une colonne de type de contrat. Les codes suivants sont mappés :  
  `FT -> "Full-time"`, `PT -> "Part-time"`, `CT -> "Contract"`, `FL -> "Freelance"`.  
  La colonne est convertie en `CategoricalArray` non ordonnée.  
  Les valeurs non reconnues sont conservées telles quelles.

- `normalize!(df, CompanySize(), UptoDown(); col = :company_size)`  
  Crée un facteur ordonné représentant la taille d’entreprise du plus petit au
  plus grand (`S < M < L`). La colonne est traitée comme composée de codes `"S"`,
  `"M"`, `"L"` et convertie en `CategoricalArray{String}` ordonnée avec les niveaux
  `["S", "M", "L"]`.

- `normalize!(df, CompanySize(), DowntoUp(); col = :company_size)`  
  Crée un facteur ordonné représentant la taille d’entreprise du plus grand au
  plus petit (`L < M < S`) avec les niveaux `["L", "M", "S"]`.

- `normalize!(df, RemoteRatio(); col = :remote_ratio, allowed = (0, 50, 100))`  
  Projette les valeurs d’un ratio de télétravail vers l’ensemble discret `allowed`
  en prenant la valeur la plus proche. Par défaut : `(0, 50, 100)`.  
  Les valeurs `missing` sont conservées.

- `normalize!(df, JobTitle(); col = :job_title, mapping = JOB_TITLE_MAPPING)`  
  Harmonise les intitulés de poste via un dictionnaire de correspondance
  `brut -> catégorie canonique`. La logique essaie la clé exacte, puis en minuscules
  si besoin. Les valeurs non trouvées dans le mapping sont conservées.

- `normalize!(df, CountryCode(); col = :country,
               mapping = COUNTRY_CODE_MAPPING,
               region_col = nothing)`  
  Convertit des pays / codes bruts en codes ISO2 cohérents à l’aide d’un dictionnaire
  externe. Si `region_col` est fourni, une colonne supplémentaire est remplie avec
  la région correspondante via `REGION_MAP`. Lorsque le mapping échoue, la valeur
  brute est conservée pour éviter de perdre de l’information.

# Arguments

- `df`    : `AbstractDataFrame` à normaliser (modifié en place).
- `field` : type de champ indiquant la normalisation à appliquer
            (`EmploymentType()`, `CompanySize()`, `RemoteRatio()`, `JobTitle()`,
             `CountryCode()`, etc.).
- `args...` / `kwargs...` : paramètres supplémentaires spécifiques à chaque type
  de normalisation (par exemple `col`, `allowed`, `mapping`, `region_col`, etc.).

# Retour

- Le même `DataFrame`, modifié en place.


# Exemples

Normalisation en place du type de contrat :

```julia
df = DataFrame(employment_type = ["FT", "PT", "CT", "FL", "XX", missing])
normalize!(df, EmploymentType())

df.employment_type isa CategoricalArray
isequal(df.employment_type[6], missing)    # missing conservé
df.employment_type[5] == "XX"              # valeur inconnue conservée telle quelle
```

Tailles d’entreprise ordonnées (du plus petit au plus grand, puis l’inverse) :

```julia
df = DataFrame(company_size = ["S", "M", "L", "M"])
normalize!(df, CompanySize(), UptoDown())
levels(df.company_size) == ["S", "M", "L"]
isordered(df.company_size) == true

df2 = DataFrame(company_size = ["S", "M", "L", "S"])
normalize!(df2, CompanySize(), DowntoUp())
levels(df2.company_size) == ["L", "M", "S"]
```

Projection d’un ratio de télétravail sur un ensemble discret :

```julia
df = DataFrame(remote_ratio = [20, 40, 80, 0, 50, 100, missing])
normalize!(df, RemoteRatio())  # allowed = (0, 50, 100) par défaut

isequal(df.remote_ratio, [0, 50, 100, 0, 50, 100, missing])
```

Normalisation des intitulés de poste :

```julia
df = DataFrame(job_title = ["Senior Data Scientist", "sr data scientist", "Unknown title", missing])
normalize!(df, JobTitle())

df.job_title[3] == "Unknown title"   # valeur inconnue conservée
isequal(df.job_title[4], missing)    # missing conservé
```

Normalisation des pays / codes et ajout d’une région :

```julia
df = DataFrame(country = ["US", "FR", "UnknownLand", missing])
normalize!(df, CountryCode(); region_col = :region)

any(n -> n == :region || n == "region", names(df))
nrow(df) == length(df.region)
isequal(df.region[4], missing)
isequal(df.region[3], missing)       # UnknownLand -> région manquante
```

# Notes

- Si aucune méthode spécialisée ne correspond à la combinaison de types fournie,
  un `ArgumentError` est levé pour indiquer qu’il n’existe pas de méthode adaptée.
- Voir également [`normalize`] pour la version non mutante qui travaille sur une copie.
# Notes
"""
function normalize!(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...)
    throw(ArgumentError("No matching normalization method for $(typeof(field)) with given arguments"))
end


"""
    normalize(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...) -> DataFrame

Version non mutante de [`normalize!`] : crée une copie de `df`, applique `normalize!`
sur cette copie, puis renvoie le résultat.

# Arguments

- `df`    : `AbstractDataFrame` d’entrée (non modifié).
- `field` : type de champ indiquant la normalisation à appliquer
            (`EmploymentType()`, `CompanySize()`, `RemoteRatio()`, `JobTitle()`, `CountryCode()`, etc.).
- `args...` / `kwargs...` : paramètres supplémentaires passés à la méthode `normalize!`
  correspondante.

# Retour

- Un nouveau `DataFrame` contenant la version normalisée de `df`.

# Notes

- Utile lorsque l’on souhaite chaîner plusieurs transformations sans muter les
  données sources.
"""
function normalize(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...)
    df2 = copy(df)
    normalize!(df2, field, args...; kwargs...)
    return df2
end

# EmploymentType

function normalize!(df::AbstractDataFrame, ::EmploymentType; col::Symbol = :employment_type)
    colname = _resolve_col(df, col)

    df[!, colname] = CategoricalArray(
        [v === missing ? missing :
         get(EMPLOYMENT_TYPE_MAPPING, String(v), String(v))
         for v in df[!, colname]];
        ordered = false,
    )
    return df
end

# CompanySize

function normalize!(df::AbstractDataFrame, ::CompanySize, ::UptoDown; col::Symbol = :company_size)
    colname = _resolve_col(df, col)

    raw = string.(df[!, colname])
    levels = ["S", "M", "L"]
    df[!, colname] = CategoricalArray(raw; levels = levels, ordered = true)
    return df
end

function normalize!(df::AbstractDataFrame, ::CompanySize, ::DowntoUp; col::Symbol = :company_size)
    colname = _resolve_col(df, col)

    raw = string.(df[!, colname])
    levels = ["L", "M", "S"]
    df[!, colname] = CategoricalArray(raw; levels = levels, ordered = true)
    return df
end


# RemoteRatio

function normalize!(df::AbstractDataFrame, ::RemoteRatio;
                    col::Symbol = :remote_ratio,
                    allowed = (0, 50, 100))
    colname = _resolve_col(df, col)

    vals = df[!, colname]
    allowed_float = Float64.(allowed)

    df[!, colname] = map(vals) do v
        if v === missing
            missing
        else
            x = Float64(v)
            allowed_float[argmin(abs.(allowed_float .- x))]
        end
    end

    return df
end


# JobTitle

function normalize!(df::AbstractDataFrame, ::JobTitle;
                    col::Symbol = :job_title,
                    mapping::AbstractDict{<:AbstractString,<:AbstractString} = JOB_TITLE_MAPPING)
    colname = _resolve_col(df, col)

    df[!, colname] = map(df[!, colname]) do v
        if v === missing
            missing
        else
            s = String(v)
            get(mapping, s, get(mapping, lowercase(s), s))
        end
    end

    return df
end

# CountryCode (+ région optionnelle)

function normalize!(df::AbstractDataFrame, ::CountryCode;
                    col::Symbol = :country,
                    mapping = COUNTRY_CODE_MAPPING,
                    region_col::Union{Symbol,Nothing} = nothing)
    colname = _resolve_col(df, col)

    n = nrow(df)
    codes = Vector{Union{Missing,String}}(undef, n)
    regions = region_col === nothing ? nothing : Vector{Union{Missing,String}}(undef, n)

    for (i, v) in pairs(df[!, colname])
        if v === missing
            codes[i] = missing
            if regions !== nothing
                regions[i] = missing
            end
        else
            s = String(v)

            # On essaie exact, upper, lower dans le mapping
            m = get(mapping, s,
                    get(mapping, uppercase(s),
                        get(mapping, lowercase(s), nothing)))

            code = if m === nothing
                # Pas trouvé: on garde la valeur d'origine
                s
            elseif m isa String
                m
            elseif m isa NamedTuple
                # Support optionnel si un mapping plus riche fournit (:code, :region)
                haskey(m, :code) ? String(m.code) : s
            else
                String(m)
            end

            codes[i] = code

            if regions !== nothing
                regions[i] = (code !== missing && haskey(REGION_MAP, code)) ?
                             REGION_MAP[code] : missing
            end
        end
    end

    df[!, colname] = codes
    if region_col !== nothing
        df[!, region_col] = regions
    end

    return df
end

# impute_missing

using DataFrames
using Statistics: mean, median
using CategoricalArrays

"""
    ImputeMethod

Type abstrait racine pour toutes les stratégies d’imputation utilisées par
`impute_missing!` / `impute_missing`. Les sous-types spécialisés définissent
le comportement pour différents types de colonnes (numériques, catégorielles,
booléennes, etc.).
"""
abstract type ImputeMethod end

"""
    NumericImputeMethod <: ImputeMethod

Type abstrait pour les stratégies d’imputation appliquées aux colonnes
numériques (`Union{Missing, Real}`), comme `NumMedian`, `NumMean` ou
`NumConstant`.
"""
abstract type NumericImputeMethod    <: ImputeMethod end
"""
    NumMedian <: NumericImputeMethod

Stratégie d’imputation numérique qui remplace les valeurs manquantes par
la médiane des valeurs observées dans la colonne.
"""
struct NumMedian   <: NumericImputeMethod end

"""
    NumMean <: NumericImputeMethod

Stratégie d’imputation numérique qui remplace les valeurs manquantes par
la moyenne des valeurs observées dans la colonne.
"""
struct NumMean     <: NumericImputeMethod end

"""
    NumConstant(value) <: NumericImputeMethod

Stratégie d’imputation numérique qui remplace les valeurs manquantes par
une constante fournie (`value`), convertie au type non-missing de la colonne.
"""
struct NumConstant <: NumericImputeMethod
    value::Float64
end

"""
    CategoricalImputeMethod <: ImputeMethod

Type abstrait pour les stratégies d’imputation appliquées aux colonnes
catégorielles ou textuelles (`Union{Missing, AbstractString}` ou
`CategoricalArray{String}`), comme `CatMode`, `CatConstant` ou `CatNewLevel`.
"""
abstract type CategoricalImputeMethod <: ImputeMethod end

"""
    CatMode <: CategoricalImputeMethod

Stratégie d’imputation catégorielle qui remplace les valeurs manquantes par
la modalité la plus fréquente (mode) observée dans la colonne.
"""
struct CatMode      <: CategoricalImputeMethod end

"""
    CatConstant(value) <: CategoricalImputeMethod

Stratégie d’imputation catégorielle qui remplace les valeurs manquantes par
une valeur de chaîne constante fournie (`value`).
"""
struct CatConstant  <: CategoricalImputeMethod
    value::String
end

"""
    CatNewLevel(label) <: CategoricalImputeMethod

Stratégie d’imputation catégorielle qui remplace les valeurs manquantes par
un nouveau niveau (`label`). Pour les `CategoricalArray`, le niveau est
ajouté aux niveaux existants si nécessaire.
"""
struct CatNewLevel  <: CategoricalImputeMethod
    label::String
end

"""
    BoolImputeMethod <: ImputeMethod

Type abstrait pour les stratégies d’imputation appliquées aux colonnes
booléennes (`Union{Missing, Bool}`).
"""
abstract type BoolImputeMethod <: ImputeMethod end

"""
    BoolMajority <: BoolImputeMethod

Stratégie d’imputation booléenne qui remplace les valeurs manquantes par la
valeur majoritaire observée dans la colonne (`true` si ex aequo).
"""
struct BoolMajority <: BoolImputeMethod end


"""
    impute_missing!(df::AbstractDataFrame;
                    cols = nothing,
                    exclude = Symbol[],
                    num_method::NumericImputeMethod = NumMedian(),
                    cat_method::CategoricalImputeMethod = CatMode(),
                    bool_method::BoolImputeMethod = BoolMajority(),
                    verbose::Bool = false)

Impute les valeurs manquantes des colonnes d’un `DataFrame` **en place**, en utilisant
des stratégies différentes selon le type de variable (numérique, booléenne, catégorielle).

Pour chaque colonne cible :

- si son type est `Union{Missing, Real}`              → on applique `num_method` ;
- si son type est `Union{Missing, Bool}`             → on applique `bool_method` ;
- si son type est `Union{Missing, AbstractString}` ou une `CategoricalArray` de chaînes
                                                      → on applique `cat_method` ;
- sinon, la colonne est laissée telle quelle (type non géré).

Les méthodes effectives sont implémentées par `impute_column!` et les sous-types
de `ImputeMethod` (`NumMedian`, `NumMean`, `NumConstant`, `CatMode`, `CatConstant`,
`CatNewLevel`, `BoolMajority`, etc.).

# Arguments

- `df`         : `AbstractDataFrame` à modifier (imputation en place).
- `cols`       :
    - `nothing` (défaut) : toutes les colonnes sont candidates ;
    - un symbole, une chaîne ou un vecteur (`[:col1, "col2", …]`) : seules ces colonnes
      seront considérées.
- `exclude`    : vecteur de noms de colonnes (`Symbol` ou convertibles) à exclure de
                  l’imputation même si elles sont dans `cols` ou dans `names(df)`.
- `num_method` : stratégie d’imputation pour les colonnes numériques, instance d’un
                 sous-type de `NumericImputeMethod` (par défaut `NumMedian()`).
- `cat_method` : stratégie d’imputation pour les colonnes catégorielles / texte,
                 instance d’un sous-type de `CategoricalImputeMethod`
                 (par défaut `CatMode()`).
- `bool_method`: stratégie d’imputation pour les colonnes booléennes, instance d’un
                 sous-type de `BoolImputeMethod` (par défaut `BoolMajority()`).
- `verbose`    : si `true`, affiche pour chaque colonne le nombre de `missing` avant
                 et après, ainsi que la méthode utilisée.

# Retour

- Le même `DataFrame` `df`, avec ses colonnes modifiées en place.

# Exemples

Imputation simple sur plusieurs types de colonnes :

```julia
df = DataFrame(
    x_num  = [1.0, missing, 3.0],
    x_str  = ["a", missing, "a"],
    x_bool = [true, missing, false]
)

impute_missing!(df)

# x_num  : les `missing` sont remplacés par la médiane (ici 2.0) ou la moyenne selon `num_method`
# x_str  : les `missing` sont remplacés par la modalité la plus fréquente ("a") avec `CatMode()`
# x_bool : les `missing` sont remplacés par la valeur majoritaire avec `BoolMajority()`
```

Imputation ciblée sur certaines colonnes, en excluant d’autres :

```julia
df = DataFrame(
    id    = [1, 2, 3],
    score = [missing, 12.0, 15.0],
    note  = [missing, "ok", "ok"]
)

impute_missing!(df;
    cols      = [:score, :note],
    exclude   = [:id],
    num_method = NumMean(),
    cat_method = CatConstant("unknown")
)
```


```julia
impute_missing!(df; verbose = true)
# impute_missing!: colonne score – 1 -> 0 missing (méthode=NumMean)
# impute_missing!: colonne note   – 1 -> 0 missing (méthode=CatConstant)
```
"""
function impute_missing!(df::AbstractDataFrame; 
    cols = nothing,
    exclude = Symbol[],
    num_method::NumericImputeMethod = NumMedian(),
    cat_method::CategoricalImputeMethod = CatMode(),
    bool_method::BoolImputeMethod = BoolMajority(),
    verbose::Bool = false,
)
    all_names = names(df)

    selected_syms = if cols === nothing
        Symbol.(all_names)
    elseif cols isa AbstractVector
        Symbol.(cols)
    else
        Symbol[Symbol(cols)]
    end

    exclude_syms = Symbol.(exclude)
    selected_syms = filter(c -> !(c in exclude_syms), selected_syms)

    for colsym in selected_syms
        colname = _resolve_col(df, colsym)
        col = df[!, colname]
        T = eltype(col)

        before_missing = count(ismissing, col)
        method_str = ""

        if T <: Union{Missing, Bool}
            impute_column!(col, bool_method)
            method_str = string(typeof(bool_method))
        elseif T <: Union{Missing, Real}
            impute_column!(col, num_method)
            method_str = string(typeof(num_method))
        elseif T <: Union{Missing, AbstractString} || col isa CategoricalArray
            impute_column!(col, cat_method)
            method_str = string(typeof(cat_method))
        else
            method_str = "none (type non géré)"
        end

        if verbose
            after_missing = count(ismissing, col)
            println("impute_missing!: colonne $(colname) – $(before_missing) -> $(after_missing) missing (méthode=$(method_str))")
        end
    end

    return df
end

"""
    impute_missing(df::AbstractDataFrame; kwargs...) -> DataFrame

Version non mutante de [`impute_missing!`] : crée une copie de `df`, applique
`impute_missing!` sur cette copie, puis renvoie le résultat.

Tous les mots-clés (`cols`, `exclude`, `num_method`, `cat_method`, `bool_method`,
`verbose`, etc.) sont passés tels quels à `impute_missing!`.

# Arguments

- `df` : `AbstractDataFrame` d’entrée (non modifié).
- `kwargs...` : mêmes mots-clés que pour [`impute_missing!`].

# Retour

- Un nouveau `DataFrame` dont les valeurs manquantes ont été imputées.

# Exemple

```julia
df = DataFrame(x = [1.0, missing, 3.0])

df2 = impute_missing(df; num_method = NumMean())

df2 !== df                    # nouvelle copie
df.x[2] === missing           # l’original n’est pas modifié
df2.x[2] == mean(skipmissing(df.x))
```
"""
function impute_missing(df::AbstractDataFrame; kwargs...)
    df2 = copy(df)
    impute_missing!(df2; kwargs...)
    return df2
end


function impute_column!(col, method::ImputeMethod)
    return col
end

function impute_column!(col::AbstractVector{<:Union{Missing, Real}}, ::NumMedian)
    vals = collect(skipmissing(col))
    if isempty(vals)
        return col
    end

    T = Base.nonmissingtype(eltype(col))
    m = median(vals)
    mT = convert(T, m)

    for i in eachindex(col)
        if ismissing(col[i])
            col[i] = mT
        end
    end

    return col
end

function impute_column!(col::AbstractVector{<:Union{Missing, Real}}, ::NumMean)
    vals = collect(skipmissing(col))
    if isempty(vals)
        return col
    end

    T = Base.nonmissingtype(eltype(col))
    m = mean(vals)
    mT = convert(T, m)

    for i in eachindex(col)
        if ismissing(col[i])
            col[i] = mT
        end
    end

    return col
end

function impute_column!(col::AbstractVector{<:Union{Missing, Real}}, m::NumConstant)
    T = Base.nonmissingtype(eltype(col))
    v = convert(T, m.value)

    for i in eachindex(col)
        if ismissing(col[i])
            col[i] = v
        end
    end

    return col
end


function impute_column!(col::AbstractVector{<:Union{Missing, AbstractString}}, ::CatMode)
    counts = Dict{String, Int}()

    for x in col
        if !ismissing(x)
            s = String(x)
            counts[s] = get(counts, s, 0) + 1
        end
    end

    if isempty(counts)
        return col
    end

    mode_val = ""
    mode_count = -1
    for (k, v) in counts
        if v > mode_count
            mode_count = v
            mode_val = k
        end
    end

    for i in eachindex(col)
        if ismissing(col[i])
            col[i] = mode_val
        end
    end

    return col
end

function impute_column!(col::AbstractVector{<:Union{Missing, AbstractString}}, m::CatConstant)
    for i in eachindex(col)
        if ismissing(col[i])
            col[i] = m.value
        end
    end
    return col
end

function impute_column!(col::AbstractVector{<:Union{Missing, AbstractString}}, m::CatNewLevel)
    for i in eachindex(col)
        if ismissing(col[i])
            col[i] = m.label
        end
    end
    return col
end


function impute_column!(col::AbstractVector{<:Union{Missing, Bool}}, ::BoolMajority)
    n_true = 0
    n_false = 0

    for x in col
        if x === true
            n_true += 1
        elseif x === false
            n_false += 1
        end
    end

    if n_true == 0 && n_false == 0
        return col
    end

    fill_val = n_true >= n_false

    for i in eachindex(col)
        if ismissing(col[i])
            col[i] = fill_val
        end
    end

    return col
end


function impute_column!(col::CategoricalVector, ::CatMode)
    best_level = nothing
    best_count = -1

    for lvl in levels(col)
        c = count(==(lvl), skipmissing(col))
        if c > best_count
            best_count = c
            best_level = lvl
        end
    end

    best_level === nothing && return col

    replace!(col, missing => best_level)
    return col
end

function impute_column!(col::CategoricalVector, m::CatConstant)
    lab = m.value
    if !(lab in levels(col))
        levels!(col, vcat(levels(col), lab))
    end
    replace!(col, missing => lab)
    return col
end

function impute_column!(col::CategoricalVector, m::CatNewLevel)
    lab = m.label
    if !(lab in levels(col))
        levels!(col, vcat(levels(col), lab))
    end
    replace!(col, missing => lab)
    return col
end

# convert_currency_to_usd


# Type abstrait pour les conversions de devises
abstract type CurrencyConversionMode end

# Modes de conversion disponibles
struct UseExchangeRates <: CurrencyConversionMode end

"""
    convert_currency_to_usd!(df, mode::CurrencyConversionMode, args...; kwargs...)
    convert_currency_to_usd(df, mode::CurrencyConversionMode, args...; kwargs...) -> DataFrame
    convert_currency_to_usd!(df, UseExchangeRates(); kwargs...)

API pour la conversion de devises en USD.

Cette fonction propose trois variantes :

- `convert_currency_to_usd!(df, mode::CurrencyConversionMode, ...)` : API générique mutante (dispatch sur le mode).
- `convert_currency_to_usd(df, mode::CurrencyConversionMode, ...)` : version non mutante qui renvoie une copie.
- `convert_currency_to_usd!(df, UseExchangeRates(); ...)` : implémentation concrète avec taux de change historiques.

# Arguments

- `df` : `AbstractDataFrame` contenant les données de salaires.
- `mode` : instance de `CurrencyConversionMode` définissant la stratégie de conversion.
  Modes disponibles :
  - `UseExchangeRates()` : utilise une table de taux de change historiques.

Pour `UseExchangeRates()`, arguments optionnels :
- `salary_col::Symbol` : colonne contenant le salaire brut (par défaut `:salary`).
- `currency_col::Symbol` : colonne contenant le code devise ISO 4217 (ex: "EUR", "GBP") (par défaut `:salary_currency`).
- `year_col::Symbol` : colonne contenant l'année de travail (par défaut `:work_year`).
- `usd_col::Symbol` : colonne où écrire le salaire converti en USD (par défaut `:salary_in_usd`).
- `exchange_rates::AbstractDataFrame` : DataFrame avec colonnes `year`, `currency`, `rate` (par défaut `EXCHANGE_RATES`).

# Comportement

Version mutante (`convert_currency_to_usd!`) :
- Modifie `df` **en place** en ajoutant une colonne de salaires convertis en USD.
- Joint les taux de change selon la devise et l'année via `leftjoin!`.
- Calcule `salary * rate` pour obtenir le montant en USD.
- Si aucun taux n'est trouvé pour une combinaison devise/année, la valeur USD sera `missing`.
- Les valeurs `missing` dans `salary_col` ou `currency_col` sont propagées dans `usd_col`.
- Vérifie l'existence des colonnes requises avant traitement via `_resolve_col`.
- Crée des colonnes temporaires `_temp_currency` et `_temp_year` pour la jointure.
- Nettoie automatiquement les colonnes temporaires après conversion.

Version non mutante (`convert_currency_to_usd`) :
- Crée une copie de `df`, applique `convert_currency_to_usd!` dessus et la renvoie.
- Le `DataFrame` original reste inchangé.

# Retour

- Version mutante : renvoie le `DataFrame` modifié (par convention).
- Version non mutante : renvoie un nouveau `DataFrame` avec la colonne convertie.

# Exceptions

- `ArgumentError` : si aucune méthode n'est définie pour le mode fourni (API générique).
- `ArgumentError` : si une colonne requise (`salary_col`, `currency_col`, `year_col`) n'existe pas dans `df` (UseExchangeRates).

# Notes

- Les noms de colonnes sont résolus en interne via `_resolve_col` pour gérer les variantes `Symbol` et `String`.
- La table `EXCHANGE_RATES` (définie dans le module de mapping) contient les taux pour 41 devises de 2020 à 2023.
- USD vers USD utilise un taux de 1.0 (pas de conversion effective).
- Les colonnes temporaires `_temp_currency`, `_temp_year`, et `rate` sont automatiquement supprimées.

# Exemples

Version mutante (modifie en place) :
```julia
df = DataFrame(
    salary = [50000, 60000],
    salary_currency = ["EUR", "USD"],
    work_year = [2022, 2022]
)

convert_currency_to_usd!(df, UseExchangeRates())
# df contient maintenant une colonne :salary_in_usd
```

Version non mutante (crée une copie) :
```julia
df = DataFrame(
    salary = [50000, 60000],
    salary_currency = ["EUR", "USD"],
    work_year = [2022, 2022]
)

df_usd = convert_currency_to_usd(df, UseExchangeRates())
# df reste inchangé, df_usd contient :salary_in_usd
```

See also [`EXCHANGE_RATES`](@ref), [`UseExchangeRates`](@ref), [`_resolve_col`](@ref)
"""
function convert_currency_to_usd! end
function convert_currency_to_usd end

# Implémentation de l'API générique (mutante)
function convert_currency_to_usd!(df::AbstractDataFrame, mode::CurrencyConversionMode, args...; kwargs...)
    throw(ArgumentError("No matching conversion method for $(typeof(mode)) with given arguments"))
end

# Implémentation de l'API générique (non mutante)
function convert_currency_to_usd(df::AbstractDataFrame, mode::CurrencyConversionMode, args...; kwargs...)
    df2 = copy(df)
    convert_currency_to_usd!(df2, mode, args...; kwargs...)
    return df2
end

# Implémentation concrète : UseExchangeRates
function convert_currency_to_usd!(df::AbstractDataFrame, ::UseExchangeRates;
                                  salary_col::Symbol = :salary,
                                  currency_col::Symbol = :salary_currency,
                                  year_col::Symbol = :work_year,
                                  usd_col::Symbol = :salary_in_usd,
                                  exchange_rates::AbstractDataFrame = EXCHANGE_RATES)

    # Vérifier que les colonnes requises existent
    required_cols = [salary_col, currency_col, year_col]
    for col in required_cols
        try
            _resolve_col(df, col)
        catch e
            throw(ArgumentError("Column $(col) not found in DataFrame"))
        end
    end

    # Résoudre les noms de colonnes (Symbol vs String)
    salary_name = _resolve_col(df, salary_col)
    currency_name = _resolve_col(df, currency_col)
    year_name = _resolve_col(df, year_col)

    # Créer une copie de travail avec des colonnes temporaires pour la jointure
    df[!, :_temp_currency] = string.(df[!, currency_name])
    df[!, :_temp_year] = Int.(df[!, year_name])

    # Préparer le DataFrame de taux avec les mêmes noms temporaires
    rates_df = copy(exchange_rates)
    rename!(rates_df, :currency => :_temp_currency, :year => :_temp_year)

    # Joindre les taux de change
    leftjoin!(df, rates_df, on = [:_temp_currency, :_temp_year])

    # Calculer le salaire en USD
    n = nrow(df)
    usd_values = Vector{Union{Missing, Float64}}(undef, n)

    for i in 1:n
        salary_val = df[i, salary_name]
        rate_val = df[i, :rate]

        if salary_val === missing || rate_val === missing
            usd_values[i] = missing
        else
            usd_values[i] = Float64(salary_val) * Float64(rate_val)
        end
    end

    # Assigner la colonne résultat
    df[!, usd_col] = usd_values

    # Nettoyer les colonnes temporaires
    select!(df, Not([:_temp_currency, :_temp_year, :rate]))

    return df
end

# export_cleaned_csv

using CSV
using DataFrames

"""
    export_cleaned(path::AbstractString, df::AbstractDataFrame; delim = ',', kwargs...)

Export the cleaned DataFrame `df` to a CSV file at the location `path`.

`delim` controls the column delimiter (`,` by default). Any additional keyword
arguments in `kwargs` are forwarded to `CSV.write`.
"""
function export_cleaned(path::AbstractString, df::AbstractDataFrame; delim = ',', kwargs...)
    return CSV.write(path, df; delim = delim, kwargs...)
end

"""
    export_cleaned(io::IO, df::AbstractDataFrame; delim = ',', kwargs...)

Export the cleaned DataFrame `df` as CSV to an open IO stream `io`.

`delim` controls the column delimiter (`,` by default). Any additional keyword
arguments in `kwargs` are forwarded to `CSV.write`.
"""
function export_cleaned(io::IO, df::AbstractDataFrame; delim = ',', kwargs...)
    return CSV.write(io, df; delim = delim, kwargs...)
end