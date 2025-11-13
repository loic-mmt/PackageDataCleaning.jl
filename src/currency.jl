# convert_currency_to_usd

import DataFrames: AbstractDataFrame, nrow, leftjoin!
using DataFrames

# Type abstrait pour les conversions de devises
abstract type CurrencyConversionMode end

# Modes de conversion disponibles
struct UseExchangeRates <: CurrencyConversionMode end

# Taux de change par défaut (2020-2023)
const EXCHANGE_RATES = DataFrame(
    year = repeat(2020:2023, inner=41),
    currency = repeat([
        "AUD", "BRL", "CAD", "CHF", "CLP", "CZK", "DKK", "EUR",
        "GBP", "HKD", "HUF", "ILS", "INR", "JPY", "MXN", "NOK",
        "SGD", "THB", "TRY", "USD", "AED", "ARS", "BGN", "CNY",
        "COP", "EGP", "IDR", "KRW", "MAD", "MYR", "NZD", "PHP",
        "PKR", "PLN", "RON", "RUB", "SAR", "SEK", "TWD", "VND", "ZAR"
    ], outer=4),
    rate = [
        # 2020
        0.65, 0.18, 0.75, 1.10, 0.0013, 0.043, 0.16, 0.88, 0.75, 0.13, 0.0028, 0.28,
        0.013, 0.0095, 0.053, 0.11, 0.72, 0.029, 0.029, 1.00, 0.27, 0.014, 0.58, 0.14,
        0.00047, 0.032, 0.000069, 0.00083, 0.11, 0.24, 0.64, 0.021, 0.0061, 0.26, 0.23,
        0.013, 0.27, 0.11, 0.033, 0.000043, 0.063,
        # 2021
        0.70, 0.19, 0.79, 1.12, 0.0014, 0.045, 0.15, 0.85, 0.73, 0.13, 0.0029, 0.28,
        0.012, 0.0091, 0.055, 0.12, 0.74, 0.030, 0.028, 1.00, 0.27, 0.010, 0.57, 0.15,
        0.00049, 0.031, 0.000070, 0.00085, 0.11, 0.24, 0.68, 0.020, 0.0059, 0.24, 0.22,
        0.014, 0.27, 0.12, 0.036, 0.000044, 0.065,
        # 2022
        0.68, 0.20, 0.78, 1.14, 0.0013, 0.046, 0.15, 0.95, 0.76, 0.13, 0.0025, 0.29,
        0.012, 0.0076, 0.054, 0.11, 0.73, 0.031, 0.030, 1.00, 0.27, 0.0074, 0.54, 0.14,
        0.00048, 0.050, 0.000064, 0.00078, 0.10, 0.22, 0.62, 0.018, 0.0057, 0.22, 0.20,
        0.017, 0.27, 0.10, 0.033, 0.000042, 0.058,
        # 2023
        0.66, 0.21, 0.77, 1.15, 0.0013, 0.045, 0.16, 0.92, 0.75, 0.13, 0.0027, 0.28,
        0.012, 0.00822, 0.053, 0.12, 0.72, 0.029, 0.029, 1.00, 0.27, 0.0028, 0.53, 0.14,
        0.00046, 0.032, 0.000065, 0.00076, 0.10, 0.21, 0.61, 0.018, 0.0056, 0.23, 0.21,
        0.011, 0.27, 0.10, 0.032, 0.000041, 0.053
    ]
)

"""
    convert_currency_to_usd!(df, mode::CurrencyConversionMode, args...; kwargs...)

API générique pour la conversion de devises en USD.
Le comportement exact dépend du `mode` passé (multiple dispatch).

Modes disponibles:
- `UseExchangeRates()` : utilise une table de taux de change historiques

Version mutante qui modifie `df` sur place.
"""
function convert_currency_to_usd!(df::AbstractDataFrame, mode::CurrencyConversionMode, args...; kwargs...)
    throw(ArgumentError("No matching conversion method for $(typeof(mode)) with given arguments"))
end

"""
    convert_currency_to_usd(df, mode::CurrencyConversionMode, args...; kwargs...) -> DataFrame

Version non mutante de `convert_currency_to_usd!`:
crée une copie de `df`, applique `convert_currency_to_usd!` dessus et la renvoie.
"""
function convert_currency_to_usd(df::AbstractDataFrame, mode::CurrencyConversionMode, args...; kwargs...)
    df2 = copy(df)
    convert_currency_to_usd!(df2, mode, args...; kwargs...)
    return df2
end

"""
    convert_currency_to_usd!(df, UseExchangeRates();
                             salary_col = :salary,
                             currency_col = :salary_currency,
                             year_col = :work_year,
                             usd_col = :salary_in_usd,
                             exchange_rates = EXCHANGE_RATES)

Convertit les salaires en USD en utilisant des taux de change historiques.

# Arguments
- `salary_col`: colonne contenant le salaire brut
- `currency_col`: colonne contenant le code devise (ex: "EUR", "GBP")
- `year_col`: colonne contenant l'année de travail
- `usd_col`: colonne où écrire le salaire converti en USD
- `exchange_rates`: DataFrame avec colonnes `year`, `currency`, `rate`

# Comportement
- Joint les taux de change selon la devise et l'année
- Calcule `salary * rate` pour obtenir le montant en USD
- Si aucun taux n'est trouvé, la valeur USD sera `missing`
- Les `missing` dans salary ou currency sont propagés

"""
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