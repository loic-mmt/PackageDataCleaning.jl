```@meta
CurrentModule = PackageDataCleaning
```

# API de PackageDataCleaning.jl

## Ingestion et validation
```@docs
import_data
SalaryTbl
_missing_columns
validate_schema
standardize_colnames!
enforce_types
deduplicate_rows
KeepFirst
DropAll
```

## Normalisation métier
```@docs
NormalMode
UptoDown
DowntoUp
NormalizeField
EmploymentType
CompanySize
RemoteRatio
JobTitle
CountryCode
normalize!
normalize
EMPLOYMENT_TYPES
EXPERIENCE
SIZE
```

## Devises
```@docs
CurrencyConversionMode
UseExchangeRates
convert_currency_to_usd!
convert_currency_to_usd
EXCHANGE_RATES
```

## Valeurs manquantes
```@docs
ImputeMethod
NumericImputeMethod
NumMedian
NumMean
NumConstant
CategoricalImputeMethod
CatMode
CatConstant
CatNewLevel
BoolImputeMethod
BoolMajority
impute_missing!
impute_missing
impute_column!
```

## Qualité et valeurs extrêmes
```@docs
validate_range
winsorize
```

## Pipelines et export
```@docs
AbstractPipelineMode
MinimalPipeline
LightCleanPipeline
StrictCleanPipeline
MLReadyPipeline
CurrencyFocusPipeline
NoImputePipeline
pipeline
export_pipeline
export_cleaned
```

## Utilitaires
```@docs
_resolve_col
```
