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