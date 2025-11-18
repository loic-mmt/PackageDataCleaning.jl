# convert_currency_to_usd


# Type abstrait pour les conversions de devises
abstract type CurrencyConversionMode end

# Modes de conversion disponibles
struct UseExchangeRates <: CurrencyConversionMode end

"""
    convert_currency_to_usd!(df, mode::CurrencyConversionMode, args...; kwargs...)
    convert_currency_to_usd(df, mode::CurrencyConversionMode, args...; kwargs...) -> DataFrame
    convert_currency_to_usd!(df, UseExchangeRates(); kwargs...)

API pour la conversion de devises en USD.

Cette fonction propose trois variantes :

- `convert_currency_to_usd!(df, mode::CurrencyConversionMode, ...)` : API générique mutante (dispatch sur le mode).
- `convert_currency_to_usd(df, mode::CurrencyConversionMode, ...)` : version non mutante qui renvoie une copie.
- `convert_currency_to_usd!(df, UseExchangeRates(); ...)` : implémentation concrète avec taux de change historiques.

# Arguments

- `df` : `AbstractDataFrame` contenant les données de salaires.
- `mode` : instance de `CurrencyConversionMode` définissant la stratégie de conversion.
  Modes disponibles :
  - `UseExchangeRates()` : utilise une table de taux de change historiques.

Pour `UseExchangeRates()`, arguments optionnels :
- `salary_col::Symbol` : colonne contenant le salaire brut (par défaut `:salary`).
- `currency_col::Symbol` : colonne contenant le code devise ISO 4217 (ex: "EUR", "GBP") (par défaut `:salary_currency`).
- `year_col::Symbol` : colonne contenant l'année de travail (par défaut `:work_year`).
- `usd_col::Symbol` : colonne où écrire le salaire converti en USD (par défaut `:salary_in_usd`).
- `exchange_rates::AbstractDataFrame` : DataFrame avec colonnes `year`, `currency`, `rate` (par défaut `EXCHANGE_RATES`).

# Comportement

Version mutante (`convert_currency_to_usd!`) :
- Modifie `df` **en place** en ajoutant une colonne de salaires convertis en USD.
- Joint les taux de change selon la devise et l'année via `leftjoin!`.
- Calcule `salary * rate` pour obtenir le montant en USD.
- Si aucun taux n'est trouvé pour une combinaison devise/année, la valeur USD sera `missing`.
- Les valeurs `missing` dans `salary_col` ou `currency_col` sont propagées dans `usd_col`.
- Vérifie l'existence des colonnes requises avant traitement via `_resolve_col`.
- Crée des colonnes temporaires `_temp_currency` et `_temp_year` pour la jointure.
- Nettoie automatiquement les colonnes temporaires après conversion.

Version non mutante (`convert_currency_to_usd`) :
- Crée une copie de `df`, applique `convert_currency_to_usd!` dessus et la renvoie.
- Le `DataFrame` original reste inchangé.

# Retour

- Version mutante : renvoie le `DataFrame` modifié (par convention).
- Version non mutante : renvoie un nouveau `DataFrame` avec la colonne convertie.

# Exceptions

- `ArgumentError` : si aucune méthode n'est définie pour le mode fourni (API générique).
- `ArgumentError` : si une colonne requise (`salary_col`, `currency_col`, `year_col`) n'existe pas dans `df` (UseExchangeRates).

# Notes

- Les noms de colonnes sont résolus en interne via `_resolve_col` pour gérer les variantes `Symbol` et `String`.
- La table `EXCHANGE_RATES` (définie dans le module de mapping) contient les taux pour 41 devises de 2020 à 2023.
- USD vers USD utilise un taux de 1.0 (pas de conversion effective).
- Les colonnes temporaires `_temp_currency`, `_temp_year`, et `rate` sont automatiquement supprimées.

# Exemples

Version mutante (modifie en place) :
```julia
df = DataFrame(
    salary = [50000, 60000],
    salary_currency = ["EUR", "USD"],
    work_year = [2022, 2022]
)

convert_currency_to_usd!(df, UseExchangeRates())
# df contient maintenant une colonne :salary_in_usd
```

Version non mutante (crée une copie) :
```julia
df = DataFrame(
    salary = [50000, 60000],
    salary_currency = ["EUR", "USD"],
    work_year = [2022, 2022]
)

df_usd = convert_currency_to_usd(df, UseExchangeRates())
# df reste inchangé, df_usd contient :salary_in_usd
```

See also [`EXCHANGE_RATES`](@ref), [`UseExchangeRates`](@ref), [`_resolve_col`](@ref)
"""
function convert_currency_to_usd! end
function convert_currency_to_usd end

# Implémentation de l'API générique (mutante)
function convert_currency_to_usd!(df::AbstractDataFrame, mode::CurrencyConversionMode, args...; kwargs...)
    throw(ArgumentError("No matching conversion method for $(typeof(mode)) with given arguments"))
end

# Implémentation de l'API générique (non mutante)
function convert_currency_to_usd(df::AbstractDataFrame, mode::CurrencyConversionMode, args...; kwargs...)
    df2 = copy(df)
    convert_currency_to_usd!(df2, mode, args...; kwargs...)
    return df2
end

# Implémentation concrète : UseExchangeRates
function convert_currency_to_usd!(df::AbstractDataFrame, ::UseExchangeRates;
                                  salary_col::Symbol = :salary,
                                  currency_col::Symbol = :salary_currency,
                                  year_col::Symbol = :work_year,
                                  usd_col::Symbol = :salary_in_usd,
                                  exchange_rates::AbstractDataFrame = EXCHANGE_RATES)

    # Vérifier que les colonnes requises existent
    required_cols = [salary_col, currency_col, year_col]
    for col in required_cols
        try
            _resolve_col(df, col)
        catch e
            throw(ArgumentError("Column $(col) not found in DataFrame"))
        end
    end

    # Résoudre les noms de colonnes (Symbol vs String)
    salary_name = _resolve_col(df, salary_col)
    currency_name = _resolve_col(df, currency_col)
    year_name = _resolve_col(df, year_col)

    # Créer une copie de travail avec des colonnes temporaires pour la jointure
    df[!, :_temp_currency] = string.(df[!, currency_name])
    df[!, :_temp_year] = Int.(df[!, year_name])

    # Préparer le DataFrame de taux avec les mêmes noms temporaires
    rates_df = copy(exchange_rates)
    rename!(rates_df, :currency => :_temp_currency, :year => :_temp_year)

    # Joindre les taux de change
    leftjoin!(df, rates_df, on = [:_temp_currency, :_temp_year])

    # Calculer le salaire en USD
    n = nrow(df)
    usd_values = Vector{Union{Missing, Float64}}(undef, n)

    for i in 1:n
        salary_val = df[i, salary_name]
        rate_val = df[i, :rate]

        if salary_val === missing || rate_val === missing
            usd_values[i] = missing
        else
            usd_values[i] = Float64(salary_val) * Float64(rate_val)
        end
    end

    # Assigner la colonne résultat
    df[!, usd_col] = usd_values

    # Nettoyer les colonnes temporaires
    select!(df, Not([:_temp_currency, :_temp_year, :rate]))

    return df
end