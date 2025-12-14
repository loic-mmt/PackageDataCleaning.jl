"""
    AbstractPipelineMode

Type abstrait racine pour représenter un "mode" de pipeline de nettoyage /
préparation des données. Chaque sous-type (par ex. `MinimalPipeline`,
`LightCleanPipeline`, `MLReadyPipeline`, etc.) encode une séquence d’étapes
spécifique (validation, dédoublonnage, imputation, normalisation, FX, …)
utilisée par [`pipeline`](@ref).
"""
abstract type AbstractPipelineMode end

"""
    MinimalPipeline <: AbstractPipelineMode

Mode de pipeline minimal : ingestion, validation optionnelle du schéma,
standardisation des noms de colonnes et inférence de types. Sert de brique
de base pour les autres pipelines.
"""
struct MinimalPipeline      <: AbstractPipelineMode end

"""
    LightCleanPipeline <: AbstractPipelineMode

Mode de pipeline "léger" pour l’exploration (EDA) : pipeline minimal +
dédoublonnage modéré (`KeepFirst()` par défaut) + imputation "soft"
(médiane, mode, majorité booléenne).
"""
struct LightCleanPipeline   <: AbstractPipelineMode end

"""
    StrictCleanPipeline <: AbstractPipelineMode

Mode de pipeline "strict" focalisé sur la qualité des données : pipeline
minimal + dédoublonnage agressif (`DropAll()` par défaut) + winsorisation
des valeurs extrêmes numériques + imputation stricte (nouveau niveau "NA"
pour les catégorielles).
"""
struct StrictCleanPipeline  <: AbstractPipelineMode end

"""
    MLReadyPipeline <: AbstractPipelineMode

Mode de pipeline "ML-ready" : s’appuie sur `StrictCleanPipeline` puis applique
des normalisations métiers (types de contrat, taille d’entreprise, télétravail,
intitulés de poste, pays) et une conversion éventuelle des salaires en USD.
"""
struct MLReadyPipeline      <: AbstractPipelineMode end

"""
    CurrencyFocusPipeline <: AbstractPipelineMode

Mode de pipeline focalisé sur les questions de devise : pipeline minimal
puis conversion des salaires en USD via les taux de change (`UseExchangeRates()`).
"""
struct CurrencyFocusPipeline <: AbstractPipelineMode end

"""
    NoImputePipeline <: AbstractPipelineMode

Mode de pipeline sans imputation : pipeline minimal + dédoublonnage (mode
configurable), en conservant explicitement les `missing` pour traitement
ultérieur.
"""
struct NoImputePipeline     <: AbstractPipelineMode end




"""
    pipeline(df::AbstractDataFrame, mode::AbstractPipelineMode; kwargs...)
    pipeline(path::AbstractString, mode::AbstractPipelineMode; load_kwargs...)
    pipeline(io::IO,           mode::AbstractPipelineMode; load_kwargs...)

Point d'entrée générique pour exécuter un pipeline de nettoyage / préparation
des données selon un "mode" donné.

- Les variantes sur `path::AbstractString` et `io::IO` chargent d’abord un CSV
  via `import_data`, puis délèguent à `pipeline(df, mode; ...)`.
- La variante sur `df::AbstractDataFrame` applique directement la séquence
  d’étapes définie par le mode (validation, dédoublonnage, imputation,
  normalisation, conversion de devise, etc.).

# Modes de pipeline

Les principaux modes fournis sont :

- [`MinimalPipeline`](@ref)  
  Pipeline minimal :  
  1. validation optionnelle du schéma (`validate_schema`) ;  
  2. standardisation des noms de colonnes (`standardize_colnames!`) ;  
  3. inférence de types (`enforce_types`).

- [`LightCleanPipeline`](@ref)  
  Pipeline "léger" pour exploration :  
  1. `MinimalPipeline` ;  
  2. dédoublonnage (`deduplicate_rows`, `KeepFirst()` par défaut) ;  
  3. imputation douce (`impute_missing!` avec `NumMedian`, `CatMode`, `BoolMajority`).  

  Mots-clés principaux :  
  - `dedup_mode::DedupMode` (par défaut `KeepFirst()`),  
  - `dedup_by` (colonnes utilisées pour la clé de déduplication),  
  - `num_method`, `cat_method`, `bool_method` pour configurer l’imputation.

- [`StrictCleanPipeline`](@ref)  
  Pipeline "strict" focalisé sur la qualité des données :  
  1. `MinimalPipeline` ;  
  2. dédoublonnage agressif (`deduplicate_rows` avec `DropAll()` par défaut) ;  
  3. winsorisation des valeurs extrêmes numériques (`winsorize`) ;  
  4. imputation stricte (`impute_missing!` avec `NumMedian`, `CatNewLevel("NA")`,
     `BoolMajority`).  

  Mots-clés principaux :  
  - `dedup_by` : colonnes utilisées pour la clé de déduplication.

- [`MLReadyPipeline`](@ref)  
  Pipeline prêt pour l’entraînement de modèles :  
  1. `StrictCleanPipeline` ;  
  2. normalisations métiers (`normalize!` avec `EmploymentType`, `CompanySize`,
     `RemoteRatio`, `JobTitle`, `CountryCode`) si les colonnes existent ;  
  3. conversion éventuelle en USD (`convert_currency_to_usd!`) selon un drapeau.  

  Mots-clés principaux :  
  - `company_size_order::NormalMode` (`UptoDown()` ou `DowntoUp()`) ;  
  - `do_currency::Bool` (par défaut `true`).

- [`CurrencyFocusPipeline`](@ref)  
  Pipeline focalisé sur la conversion de devise :  
  1. `MinimalPipeline` ;  
  2. conversion en USD (`convert_currency_to_usd!`).  

- [`NoImputePipeline`](@ref)  
  Pipeline sans imputation :  
  1. `MinimalPipeline` ;  
  2. dédoublonnage (`deduplicate_rows`, mode configurable).  
  Les `missing` sont conservés pour un traitement ultérieur.

# Arguments

- `df`   : `AbstractDataFrame` déjà chargé.
- `path` : chemin vers un fichier CSV brut.
- `io`   : flux IO contenant des données CSV (par ex. `IOBuffer`, fichier ouvert).
- `mode` : instance d’un sous-type de [`AbstractPipelineMode`](@ref) indiquant
           la séquence d’étapes à appliquer.
- `kwargs...` / `load_kwargs...` :
    - pour `df` : mots-clés spécifiques au mode (validation, dédup, imputation, FX, etc.) ;
    - pour `path` / `io` : options de chargement, transmises à `import_data`
      (par ex. `delim=','`, `header=true`, …).

# Retour

- Un nouveau `DataFrame` résultant de l’application du pipeline choisi.

# Exemples

Pipeline ML complet depuis un chemin :

```julia
df_ml = pipeline("data/raw_salaries.csv", MLReadyPipeline();
                 required_columns = [:work_year, :salary, :salary_currency],
                 strict = true)
```

Pipeline léger pour EDA, à partir d’un DataFrame déjà chargé :

```julia
df_light = pipeline(df, LightCleanPipeline();
                    dedup_by   = [:company_name, :job_title],
                    num_method = NumMean())
```

Pipeline strict sans configuration particulière (par défaut) :

```julia
required = [:work_year, :salary, :salary_currency]
df_strict = pipeline("data/raw_salaries.csv",
                     StrictCleanPipeline();
                     required_columns = required,
                     strict = true)
```

Pipeline focalisé sur les devises :

```julia
df_fx = pipeline(df, CurrencyFocusPipeline();
                 required_columns = [:salary, :salary_currency])
```
"""
function pipeline(df::AbstractDataFrame, mode::AbstractPipelineMode; kwargs...)
    throw(ArgumentError("No pipeline implementation defined for mode $(typeof(mode))"))
end

function pipeline(path::AbstractString, mode::AbstractPipelineMode; load_kwargs...)
    df = import_data(path; load_kwargs...)
    return pipeline(df, mode)
end

function pipeline(io::IO, mode::AbstractPipelineMode; load_kwargs...)
    df = import_data(io; load_kwargs...)
    return pipeline(df, mode)
end



function pipeline(df::AbstractDataFrame, ::MinimalPipeline;
                  required_columns=nothing,
                  strict::Bool=true)
    if required_columns !== nothing
        validate_schema(df, required_columns; strict=strict)
    end

    # Noms de colonnes propres
    standardize_colnames!(df)

    # Vérif des types
    df2 = enforce_types(df)
    return df2
end



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

    # Étape 3 : cap des valeurs extrêmes sur les colonnes numériques
    df2 = winsorize(df2)

    # Étape 4 : imputation stricte
    impute_missing!(df2;
        num_method  = NumMedian(),
        cat_method  = CatNewLevel("NA"),
        bool_method = BoolMajority(),
    )

    return df2
end



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



function pipeline(df::AbstractDataFrame, ::CurrencyFocusPipeline;
                  required_columns=nothing,
                  strict::Bool=true)
    df2 = pipeline(df, MinimalPipeline();
                   required_columns=required_columns,
                   strict=strict)

    convert_currency_to_usd!(df2, UseExchangeRates())
    return df2
end



function pipeline(df::AbstractDataFrame, ::NoImputePipeline;
                  required_columns=nothing,
                  strict::Bool=true,
                  dedup_mode::DedupMode = KeepFirst(),
                  dedup_by = nothing)
    df2 = pipeline(df, MinimalPipeline();
                   required_columns=required_columns,
                   strict=strict)

    # Si l’utilisateur ne précise pas de colonnes, on déduplique sur toutes les colonnes.
    by_cols = dedup_by === nothing ? names(df2) : dedup_by 
    df2 = deduplicate_rows(df2, dedup_mode; by=by_cols)

    return df2
end





"""
    export_pipeline(in_path::AbstractString,
                    mode::AbstractPipelineMode,
                    out_path::AbstractString;
                    load_delim = ',',
                    export_delim = ',') -> DataFrame

Exécute un pipeline de nettoyage complet puis exporte le résultat dans un CSV.

1. Charge le CSV brut depuis `in_path` avec `import_data`.
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
    df = import_data(in_path; delim = load_delim)

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