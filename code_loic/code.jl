

""""Cette fonction peut être utilisée dans le cas ou nous avons un fichier ouvert stocké dnas la RAM de l'ordinateur. 
Voici des exemples de cas où cette fonction peut être utile:
    •   Pour les tests : tester la fonction de lecture CSV sans créer de fichiers.
	•	Pour des données générées : si on génères du texte CSV dans le code, on peut directement le mettre dans un IOBuffer et l'utiliser ici.
	•	Pour d'autres cas : lecture depuis une API et autre.

Exemple d'utilisation :

        text = "
        col1,col2
        1,2
        3,4
        "

        buf = IOBuffer(text)    
        df = load_raw_csv(buf) 
"""


#validate_schema
"""
Exemple d'utiliation :

df = DataFrame(a = [1], b = [2])
validate_schema(df, [:a, :b, :c])
# ArgumentError -> Missing required columns: c

validate_schema(df, [:a, :b, :c]; strict=false)
# missing c
"""

#standardize_colnames!
"""
Exemple d'utilisation : 
df = DataFrame("  My Col (1) " => [1,2], "SALAIRE (€)" => [10,20])
standardize_colnames!(df)
names(df) 
# "my_col_1", "salaire"
"""

#enforce_types
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

#deduplicate_rows
"""
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















using DataFrames
using Statistics: mean, median
using CategoricalArrays



# impute_missing

#equivalent en R
"""
#' Impute missing values for numeric and categorical columns
#'
#' Impute `NA` values column-wise with simple, dependency-free strategies.
#' Numeric: "median", "mean" ou "constant".
#' Catégorielles (character/factor): "mode", "constant" ou "new_level" (étiquette par défaut: "Missing").
#' Logiques: imputées à la valeur majoritaire.
#'
#' @param data data.frame d'entrée.
#' @param cols (optionnel) noms de colonnes à imputer. Par défaut: toutes.
#' @param exclude (optionnel) colonnes à exclure après `cols`.
#' @param num_method "median", "mean" ou "constant". Défaut: "median".
#' @param cat_method "mode", "constant" ou "new_level". Défaut: "mode".
#' @param num_constant valeur numérique utilisée si `num_method="constant"` (ou si toute la colonne est NA). Défaut: 0.
#' @param cat_constant valeur utilisée si `cat_method` est "constant" ou "new_level" (ou si toute la colonne est NA). Défaut: "Missing".
#' @param verbose afficher un résumé par colonne. Défaut: TRUE.
#' @return Le data.frame avec les NA imputés
"""

# 1. Déterminer les colonnes à traiter
#    - si cols == nothing: toutes
#    - sinon: seulement celles dans cols
#    - retirer celles de exclude

# 2. Pour chaque colname:
#      v = df[!, colname]
#      if eltype(v) <: Union{Missing, Real}
#          impute_column!(v, num_method)
#      elseif eltype(v) <: Union{Missing, Bool}
#          impute_column!(v, bool_method)
#      elseif eltype(v) <: Union{Missing, AbstractString} || v isa CategoricalArray
#          impute_column!(v, cat_method)
#      else
#          impute_column!(v, some_default_method) # ou ne rien faire
#      end
#
# 3. Retourner df

abstract type ImputeMethod end

"Stratégies numériques"
abstract type NumericImputeMethod    <: ImputeMethod end
struct NumMedian   <: NumericImputeMethod end
struct NumMean     <: NumericImputeMethod end
struct NumConstant <: NumericImputeMethod
    value::Float64
end

"Stratégies catégorielles"
abstract type CategoricalImputeMethod <: ImputeMethod end
struct CatMode      <: CategoricalImputeMethod end
struct CatConstant  <: CategoricalImputeMethod
    value::String
end
struct CatNewLevel  <: CategoricalImputeMethod
    label::String
end

"Stratégie booléenne / logique"
abstract type BoolImputeMethod <: ImputeMethod end
struct BoolMajority <: BoolImputeMethod end


"""
    impute_missing!(df; 
        cols = nothing,
        exclude = Symbol[],
        num_method::NumericImputeMethod = NumMedian(),
        cat_method::CategoricalImputeMethod = CatMode(),
        bool_method::BoolImputeMethod = BoolMajority(),
        verbose::Bool = false)

Impute les valeurs manquantes dans `df` en place.
"""
function impute_missing!(df::AbstractDataFrame; 
    cols = nothing,
    exclude = Symbol[],
    num_method::NumericImputeMethod = NumMedian(),
    cat_method::CategoricalImputeMethod = CatMode(),
    bool_method::BoolImputeMethod = BoolMajority(),
    verbose::Bool = false,
)
    # TODO:
    # 1. Déterminer les colonnes cibles (en fonction de cols/exclude)
    # 2. Boucler sur chaque colonne cible:
    #      - router vers impute_column!(...) en fonction du type + méthode
    # 3. Si verbose, afficher un résumé
    all_cols = names(df)
    target_cols = if cols === nothing
        all_cols
    else
        if cols isa AbstractVector
            Symbol.(cols)
        else
            Symbol[Symbol(cols)]
        end
    end

    exclude_syms = Symbol.(exclude)
    target_cols = filter(c -> !(c in exclude_syms), target_cols)

    for colname in target_cols
        col = df[!, colname]
        T = eltype(col)

        before_missing = count(ismissing, col)

        if T <: Union{Missing, Real}
            impute_column!(col, num_method)
            method_str = string(typeof(num_method))
        elseif T <: Union{Missing, Bool}
            impute_column!(col, bool_method)
            method_str = string(typeof(bool_method))
        elseif T <: Union{Missing, AbstractString} || col isa CategoricalArray
            impute_column!(col, cat_method)
            method_str = string(typeof(cat_method))
        else 
            method_str = "none (type non géré)"
        end

        if verbose
            after_missing = count(ismissing, col)
            println("impute_missing!: colonne $(colname) - $(before_missing) -> $(after_missing) missing (méthode = $(method_str))")
        end
    end

    return df
end

"""
    impute_missing(df; kwargs...) -> DataFrame

Version non mutante : renvoie une copie imputée.
"""
function impute_missing(df::AbstractDataFrame; kwargs...)
    df2 = copy(df)
    impute_missing!(df2; kwargs...)
    return df2
end


"""
    impute_column!(col, method)

Impute une seule colonne en place selon la méthode donnée.
Cette fonction est spécialisée par type d'éléments et type de méthode.
"""
function impute_column!(col, method::ImputeMethod)
    # TODO: par défaut, soit ne rien faire, soit throw pour types non gérés.
    return col
end

"Imputation numérique par médiane."
function impute_column!(col::AbstractVector{<:Union{Missing, Real}}, ::NumMedian)
    # TODO: calculer la médiane des valeurs non-missing et remplacer les missing.

    return col
end

"Imputation numérique par moyenne."
function impute_column!(col::AbstractVector{<:Union{Missing, Real}}, ::NumMean)
    # TODO
    return col
end

"Imputation numérique avec constante donnée."
function impute_column!(col::AbstractVector{<:Union{Missing, Real}}, m::NumConstant)
    # TODO: utiliser m.value
    return col
end


"Imputation catégorielle par modalité majoritaire."
function impute_column!(col::AbstractVector{<:Union{Missing, AbstractString}}, ::CatMode)
    # TODO: trouver la valeur la plus fréquente (hors missing) et l'utiliser
    return col
end

"Imputation catégorielle par constante."
function impute_column!(col::AbstractVector{<:Union{Missing, AbstractString}}, m::CatConstant)
    # TODO: remplacer missing par m.value
    return col
end

"Imputation catégorielle par nouveau niveau."
function impute_column!(col::AbstractVector{<:Union{Missing, AbstractString}}, m::CatNewLevel)
    # TODO: remplacer missing par m.label
    # Si col est CategoricalArray, penser à ajouter le niveau
    return col
end


"Imputation pour colonnes booléennes: valeur majoritaire."
function impute_column!(col::AbstractVector{<:Union{Missing, Bool}}, ::BoolMajority)
    # TODO: compter true/false, choisir le plus fréquent
    return col
end


"Fallback : types non gérés -> on ne touche pas."
function impute_column!(col::AbstractVector, method::ImputeMethod)
    # Par exemple : ne rien faire, ou éventuellement un @info si verbose global
    return col
end