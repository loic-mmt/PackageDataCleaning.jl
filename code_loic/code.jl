"""
Fonction interne qui applique toutes les étapes de nettoyage :
- standardisation des noms de colonnes
- validation du schéma (optionnel)
- enforcement des types
- conversion de devises en USD (si les colonnes salary/currency/year existent)
- suppression des doublons

Appelée par toutes les variantes de `cleaning_pipeline`.

Fonctions et modes différents : 
       load_raw_csv,
       validate_schema,
       standardize_colnames!,
       enforce_types,
       deduplicate_rows,
       _resolve_col,
       DropAll,
       KeepFirst,
       normalize,
       normalize!,
       EmploymentType,
       CompanySize,
       UptoDown,
       DowntoUp,
       RemoteRatio,
       JobTitle,
       CountryCode,
       UseExchangeRates,
       convert_currency_to_usd!,
       convert_currency_to_usd,
       CurrencyConversionMode,
       ImputeMethod,
       NumericImputeMethod,
       NumMedian,
       NumMean,
       NumConstant,
       CategoricalImputeMethod,
       CatMode,
       CatConstant,
       CatNewLevel,
       BoolImputeMethod,
       BoolMajority,
       impute_missing!,
       impute_missing,
       impute_column!
"""