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


function impute_column!(col::CategoricalArray{T,1,<:Union{Missing, T}}, ::CatMode) where {T<:AbstractString}
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

function impute_column!(col::CategoricalArray{T,1,<:Union{Missing, T}}, m::CatConstant) where {T<:AbstractString}
    lab = m.value
    if !(lab in levels(col))
        levels!(col, vcat(levels(col), lab))
    end
    replace!(col, missing => lab)
    return col
end

function impute_column!(col::CategoricalArray{T,1,<:Union{Missing, T}}, m::CatNewLevel) where {T<:AbstractString}
    lab = m.label
    if !(lab in levels(col))
        levels!(col, vcat(levels(col), lab))
    end
    replace!(col, missing => lab)
    return col
end