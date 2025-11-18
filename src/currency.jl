# convert_currency_to_usd

import DataFrames: AbstractDataFrame, nrow, leftjoin!
using DataFrames

# Type abstrait pour les conversions de devises
abstract type CurrencyConversionMode end

# Modes de conversion disponibles
struct UseExchangeRates <: CurrencyConversionMode end

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