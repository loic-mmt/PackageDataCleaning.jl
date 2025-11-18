```@meta
CurrentModule = PackageDataCleaning
```

# API de PackageDataCleaning.jl

## Fonctions de validation
```@docs
_missing_columns
validate_schema
standardize_colnames!
enforce_types
deduplicate_rows
```

## Fonctions de normalisation
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
```

## Fonctions de taux de change
```@docs
convert_currency_to_usd!
convert_currency_to_usd
```
## Autres fonctions utiles
```@docs
load_raw_csv
```