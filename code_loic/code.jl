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