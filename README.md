# PackageDataCleaning

Librairie Julia pour nettoyer et préparer rapidement des données tabulaires (DataFrames). Elle propose des briques indépendantes (validation de schéma, dédoublonnage, imputation, normalisation métier, conversion de devises) et des pipelines prêts à l’emploi.

## Installation

```julia
pkg> add PackageDataCleaning
```

## Aperçu rapide

```julia
using PackageDataCleaning
using DataFrames

df = DataFrame(
    work_year       = [2022, 2022, 2023],
    salary          = [50_000, 60_000, 45_000],
    salary_currency = ["EUR", "USD", "GBP"],
    employment_type = ["FT", "FT", "CT"],
    company_size    = ["M", "L", "S"],
    remote_ratio    = [0, 50, 100],
)

# Pipeline léger : standardisation des colonnes, typage, dédoublonnage, imputation
clean_light = pipeline(df, LightCleanPipeline(); dedup_by = [:employment_type, :company_size])

# Pipeline ML complet : nettoyage strict + normalisations + conversion USD
clean_ml = pipeline(df, MLReadyPipeline(); required_columns = [:salary, :salary_currency])
```

## Fonctionnalités principales

- Ingestion & schéma : `import_data`, `SalaryTbl`, `validate_schema`, `standardize_colnames!`, `enforce_types`
- Qualité & doublons : `deduplicate_rows`, `validate_range`, `winsorize`
- Valeurs manquantes : `impute_missing!` / `impute_missing` (numérique, catégoriel, booléen)
- Normalisation métier : `normalize!` pour types de contrat, tailles d’entreprise, remote ratio, intitulés de poste, codes pays
- Devises : `convert_currency_to_usd!` avec `UseExchangeRates()`
- Pipelines prêts à l’emploi : `MinimalPipeline`, `LightCleanPipeline`, `StrictCleanPipeline`, `MLReadyPipeline`, `CurrencyFocusPipeline`, `NoImputePipeline`
- Export : `export_cleaned` et `export_pipeline`
