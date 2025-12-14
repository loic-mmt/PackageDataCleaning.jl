```@meta
CurrentModule = PackageDataCleaning
```

# PackageDataCleaning

PackageDataCleaning.jl est une librairie Julia pour nettoyer et préparer des données tabulaires (CSV) avec DataFrames : ingestion, validation de schéma, dédoublonnage, imputation, normalisations métiers, conversion de devises et pipelines prêts à l’emploi.

## Installation

```julia
pkg> add PackageDataCleaning
```

## Fonctionnalités clés

- Ingestion & schéma : `import_data`, `SalaryTbl`, `validate_schema`, `standardize_colnames!`, `enforce_types`
- Qualité & doublons : `deduplicate_rows`, `validate_range`, `winsorize`
- Valeurs manquantes : `impute_missing!` / `impute_missing` avec stratégies numériques, catégorielles et booléennes
- Normalisation métier : `normalize!` pour les types de contrat, tailles d’entreprise, remote ratio, intitulés de poste et codes pays
- Devises : `convert_currency_to_usd!` (taux historiques intégrés)
- Pipelines : `pipeline` avec `MinimalPipeline`, `LightCleanPipeline`, `StrictCleanPipeline`, `MLReadyPipeline`, `CurrencyFocusPipeline`, `NoImputePipeline`
- Export : `export_cleaned` et `export_pipeline`

## Prise en main rapide

```julia
using PackageDataCleaning
using DataFrames

df = DataFrame(
    work_year         = [2022, 2022, 2023],
    salary            = [50_000, 60_000, 45_000],
    salary_currency   = ["EUR", "USD", "GBP"],
    employment_type   = ["FT", "FT", "CT"],
    company_size      = ["M", "L", "S"],
    remote_ratio      = [0, 50, 100],
    job_title         = ["Data Scientist", "ML Engineer", "Data Analyst"],
    employee_residence = ["FR", "US", "GB"],
    company_location  = ["FR", "US", "GB"],
)

# Pipeline prêt pour la modélisation (normalisation + conversion USD)
clean_ml = pipeline(df, MLReadyPipeline(); required_columns = [:salary, :salary_currency])

# Ou exécuter un nettoyage léger sans conversion
clean_light = pipeline(df, LightCleanPipeline(); dedup_by = [:job_title, :company_location])
```

### Où trouver quoi ?

**Introduction** : vue d’ensemble et exemples simples.
- **API** : description détaillée de chaque fonction (types, arguments, etc.).

Consulte la page `API` pour la liste complète des fonctions disponibles.

Documentation for [PackageDataCleaning](https://github.com/loic-mmt/PackageDataCleaning.jl).
