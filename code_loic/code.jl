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