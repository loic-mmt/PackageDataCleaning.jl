"""
Fonction interne qui applique toutes les étapes de nettoyage


Fonctions et modes différents : 
load_raw_csv, load_raw_csv(IO)
validate_schema
standardize_colnames!
enforce_types
deduplicate_rows, DropAll, KeepFirst
normalize, normalize!, UptoDown, DowntoUp, EmploymentType, CompanySize, UptoDown, DowntoUp, RemoteRatio, JobTitle, CountryCode
UseExchangeRates, convert_currency_to_usd!, convert_currency_to_usd, CurrencyConversionMode
ImputeMethod, NumericImputeMethod, NumMedian, NumMean, NumConstant,
CategoricalImputeMethod, CatMode, CatConstant, CatNewLevel,
BoolImputeMethod, BoolMajority,
impute_missing!, impute_missing, impute_column!
export_cleaned, export_cleaned(IO)


	1.	MinimalPipeline – ingestion + validation.
	2.	LightCleanPipeline – nettoyage léger (KeepFirst + imputation soft).
	3.	StrictCleanPipeline – qualité max, duplicates agressifs, imputation stricte, catégories “NA”.
	4.	MLReadyPipeline – prêt pour modèle (FX, features métiers, normalisation, imputation orientée ML).
	5.	CurrencyFocusPipeline – conversion de devises uniquement.
	6.	NoImputePipeline – nettoyage sans imputation, pour laisser les missing visibles.

"""
abstract type AbstractPipelineMode end

struct MinimalPipeline      <: AbstractPipelineMode end
struct LightCleanPipeline   <: AbstractPipelineMode end
struct StrictCleanPipeline  <: AbstractPipelineMode end
struct MLReadyPipeline      <: AbstractPipelineMode end
struct CurrencyFocusPipeline <: AbstractPipelineMode end
struct NoImputePipeline     <: AbstractPipelineMode end




"""
    pipeline(df::AbstractDataFrame, mode::AbstractPipelineMode; kwargs...)
    pipeline(path::AbstractString, mode::AbstractPipelineMode; load_kwargs...)
    pipeline(io::IO, mode::AbstractPipelineMode; load_kwargs...)

Point d'entrée générique pour exécuter un pipeline de nettoyage/normalisation.

- Les méthodes sur `path::AbstractString` et `io::IO` chargent d'abord un CSV
  via `load_raw_csv`, puis délèguent à la version `pipeline(df, mode; ...)`.
- Les méthodes spécialisées sur chaque `*Pipeline` définissent les étapes
  appliquées (validation, dédoublonnage, imputation, normalisation, FX, etc.).
"""
function pipeline(df::AbstractDataFrame, mode::AbstractPipelineMode; kwargs...)
    throw(ArgumentError("No pipeline implementation defined for mode $(typeof(mode))"))
end

function pipeline(path::AbstractString, mode::AbstractPipelineMode; load_kwargs...)
    df = load_raw_csv(path; load_kwargs...)
    return pipeline(df, mode)
end

function pipeline(io::IO, mode::AbstractPipelineMode; load_kwargs...)
    df = load_raw_csv(io; load_kwargs...)
    return pipeline(df, mode)
end


"""
    pipeline(df::AbstractDataFrame, ::MinimalPipeline; required_columns=nothing, strict::Bool=true)

Pipeline minimal : ingestion + validation + normalisation des noms de colonnes
+ inférence de types.

- `required_columns` : liste de colonnes attendues ; si `nothing`, pas de validation.
- `strict` : si `true`, `validate_schema` lève une erreur si des colonnes manquent.
"""
function pipeline(df::AbstractDataFrame, ::MinimalPipeline;
                  required_columns=nothing,
                  strict::Bool=true)
    if required_columns !== nothing
        validate_schema(df, required_columns; strict=strict)
    end

    # Noms de colonnes propres
    standardize_colnames!(df)

    # Inférence de types (retourne une copie)
    df2 = enforce_types(df)
    return df2
end


"""
    pipeline(df::AbstractDataFrame, ::LightCleanPipeline; kwargs...)

Pipeline "léger" pour exploration :
1. `MinimalPipeline` (ingestion + validation + types)
2. dédoublonnage (par défaut `KeepFirst()`)
3. imputation soft (médiane, mode, majorité booléenne).

Mots-clés utiles :
- `required_columns`, `strict` : passés à `MinimalPipeline`.
- `dedup_mode` :: `DedupMode` (par défaut `KeepFirst()`).
- `dedup_by`   : colonnes utilisées pour la clé (par défaut toutes les colonnes).
- `num_method`, `cat_method`, `bool_method` : stratégies d’imputation.
"""
function pipeline(df::AbstractDataFrame, ::LightCleanPipeline;
                  required_columns=nothing,
                  strict::Bool=true,
                  dedup_mode::DedupMode = KeepFirst(),
                  dedup_by = nothing,
                  num_method::NumericImputeMethod = NumMedian(),
                  cat_method::CategoricalImputeMethod = CatMode(),
                  bool_method::BoolImputeMethod = BoolMajority())
    # Étape 1 : pipeline minimal
    df2 = pipeline(df, MinimalPipeline();
                   required_columns=required_columns,
                   strict=strict)

    # Étape 2 : dédoublonnage
    by_cols = dedup_by === nothing ? names(df2) : dedup_by
    df2 = deduplicate_rows(df2, dedup_mode; by=by_cols)

    # Étape 3 : imputation légère
    impute_missing!(df2;
        num_method  = num_method,
        cat_method  = cat_method,
        bool_method = bool_method,
    )

    return df2
end


"""
    pipeline(df::AbstractDataFrame, ::StrictCleanPipeline; kwargs...)

Pipeline "strict" : qualité max.
1. `MinimalPipeline`.
2. dédoublonnage agressif (`DropAll()` par défaut).
3. winsorisation (cap des valeurs extrêmes numériques).
4. imputation stricte (nouveau niveau "NA" pour les catégorielles).
"""
function pipeline(df::AbstractDataFrame, ::StrictCleanPipeline;
                  required_columns=nothing,
                  strict::Bool=true,
                  dedup_by = nothing)
    # Étape 1 : pipeline minimal
    df2 = pipeline(df, MinimalPipeline();
                   required_columns=required_columns,
                   strict=strict)

    # Étape 2 : dédoublonnage agressif
    by_cols = dedup_by === nothing ? names(df2) : dedup_by
    df2 = deduplicate_rows(df2, DropAll(); by=by_cols)

    # Étape 3 : cap des valeurs extrêmes (winsorisation) sur colonnes numériques
    df2 = winsorize(df2)

    # Étape 4 : imputation stricte
    impute_missing!(df2;
        num_method  = NumMedian(),
        cat_method  = CatNewLevel("NA"),
        bool_method = BoolMajority(),
    )

    return df2
end


"""
    pipeline(df::AbstractDataFrame, ::MLReadyPipeline; kwargs...)

Pipeline "ML-ready" : prêt pour modèle.
1. `StrictCleanPipeline`.
2. normalisations métiers (`EmploymentType`, `CompanySize`, `RemoteRatio`,
   `JobTitle`, `CountryCode`) si les colonnes existent.
3. conversion en USD via `UseExchangeRates()` (optionnelle).

Mots-clés utiles :
- `required_columns`, `strict` : passés à `StrictCleanPipeline`.
- `company_size_order` :: `NormalMode` (`UptoDown()` ou `DowntoUp()`).
- `do_currency` :: Bool (par défaut `true`).
"""
function pipeline(df::AbstractDataFrame, ::MLReadyPipeline;
                  required_columns=nothing,
                  strict::Bool=true,
                  company_size_order::NormalMode = UptoDown(),
                  do_currency::Bool = true)
    # Étape 1 : strict cleaning
    df2 = pipeline(df, StrictCleanPipeline();
                   required_columns=required_columns,
                   strict=strict)

    # Étape 2 : normalisations métiers (appliquées seulement si les colonnes existent)
    nms = names(df2)

    if :employment_type in nms
        normalize!(df2, EmploymentType(); col = :employment_type)
    end
    if :company_size in nms
        normalize!(df2, CompanySize(), company_size_order; col = :company_size)
    end
    if :remote_ratio in nms
        normalize!(df2, RemoteRatio(); col = :remote_ratio)
    end
    if :job_title in nms
        normalize!(df2, JobTitle(); col = :job_title)
    end
    if :country in nms
        normalize!(df2, CountryCode(); col = :country)
    end

    # Étape 3 : conversion de devise
    if do_currency
        convert_currency_to_usd!(df2, UseExchangeRates())
    end

    return df2
end


"""
    pipeline(df::AbstractDataFrame, ::CurrencyFocusPipeline; kwargs...)

Pipeline focalisé sur la conversion de devises :
1. `MinimalPipeline`.
2. conversion vers USD via `UseExchangeRates()`.
"""
function pipeline(df::AbstractDataFrame, ::CurrencyFocusPipeline;
                  required_columns=nothing,
                  strict::Bool=true)
    df2 = pipeline(df, MinimalPipeline();
                   required_columns=required_columns,
                   strict=strict)

    convert_currency_to_usd!(df2, UseExchangeRates())
    return df2
end


"""
    pipeline(df::AbstractDataFrame, ::NoImputePipeline; kwargs...)

Pipeline sans imputation :
1. `MinimalPipeline`.
2. dédoublonnage (mode configurable).

Les `missing` sont conservés pour être traités plus tard par l’utilisateur
ou par un modèle plus sophistiqué.
"""
function pipeline(df::AbstractDataFrame, ::NoImputePipeline;
                  required_columns=nothing,
                  strict::Bool=true,
                  dedup_mode::DedupMode = KeepFirst(),
                  dedup_by = nothing)
    df2 = pipeline(df, MinimalPipeline();
                   required_columns=required_columns,
                   strict=strict)

    by_cols = dedup_by === nothing ? names(df2) : dedup_by
    df2 = deduplicate_rows(df2, dedup_mode; by=by_cols)

    return df2
end

""" Exemple d'utilisation
# 1) Depuis un chemin de fichier, pipeline ML complet :
df_ml = pipeline("data/raw_salaries.csv", MLReadyPipeline())

# 2) Pipeline léger pour EDA, depuis un DataFrame déjà chargé :
df_light = pipeline(df, LightCleanPipeline(); dedup_by = [:company_name, :job_title])

# 3) Pipeline strict avec validation de schéma :
required = [:work_year, :salary, :salary_currency]
df_strict = pipeline("data/raw_salaries.csv",
                     StrictCleanPipeline();
                     required_columns = required,
                     strict = true)

# 4) Juste conversion USD :
df_fx = pipeline(df, CurrencyFocusPipeline())
"""

# -------------------------------------------------------------------
# Export pipeline utilities

"""
    export_pipeline(in_path::AbstractString,
                    mode::AbstractPipelineMode,
                    out_path::AbstractString;
                    load_delim = ',',
                    export_delim = ',') -> DataFrame

Exécute un pipeline de nettoyage complet puis exporte le résultat dans un CSV.

1. Charge le CSV brut depuis `in_path` avec `load_raw_csv`.
2. Applique `pipeline(df, mode)` pour exécuter le pipeline choisi.
3. Exporte le `DataFrame` nettoyé vers `out_path` avec `export_cleaned`.

# Arguments

- `in_path`     : chemin du fichier CSV brut.
- `mode`        : mode de pipeline (`MinimalPipeline()`, `LightCleanPipeline()`,
                  `StrictCleanPipeline()`, `MLReadyPipeline()`, etc.).
- `out_path`    : chemin du fichier CSV de sortie.
- `load_delim`  : délimiteur utilisé pour lire le CSV d'entrée (par défaut `','`).
- `export_delim`: délimiteur utilisé pour écrire le CSV de sortie (par défaut `','`).

# Retour

- Le `DataFrame` nettoyé qui a été exporté.

# Exemple

```julia
cleaned = export_pipeline("data/raw_salaries.csv",
                          MLReadyPipeline(),
                          "data/clean/salaries_ml.csv")
```
"""
function export_pipeline(in_path::AbstractString,
                         mode::AbstractPipelineMode,
                         out_path::AbstractString;
                         load_delim::Char = ',',
                         export_delim::Char = ',')
    # 1) Chargement brut
    df = load_raw_csv(in_path; delim = load_delim)

    # 2) Application du pipeline
    df_clean = pipeline(df, mode)

    # 3) Export
    export_cleaned(out_path, df_clean; delim = export_delim)

    return df_clean
end


"""
    export_pipeline(df::AbstractDataFrame,
                    mode::AbstractPipelineMode,
                    out_path::AbstractString;
                    export_delim = ',') -> DataFrame

Variante d'`export_pipeline` lorsqu'on dispose déjà d'un `DataFrame` en
mémoire :

1. Applique `pipeline(df, mode)`.
2. Exporte le `DataFrame` nettoyé vers `out_path`.

# Arguments

- `df`          : `DataFrame` brut déjà chargé.
- `mode`        : mode de pipeline à appliquer.
- `out_path`    : chemin du CSV de sortie.
- `export_delim`: délimiteur utilisé pour écrire le CSV de sortie.

# Retour

- Le `DataFrame` nettoyé qui a été exporté.
"""
function export_pipeline(df::AbstractDataFrame,
                         mode::AbstractPipelineMode,
                         out_path::AbstractString;
                         export_delim::Char = ',')
    df_clean = pipeline(df, mode)
    export_cleaned(out_path, df_clean; delim = export_delim)
    return df_clean
end