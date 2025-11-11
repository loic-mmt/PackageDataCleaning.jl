#cleaning_pipeline, finalize_salary_tbl

module CleaningPipeline

using DataFrames
export cleaning_pipeline

"""
    _clean(df::DataFrame; kwargs...) -> DataFrame

Fonction interne qui applique toutes les étapes de nettoyage :
- standardisation des noms de colonnes
- validation du schéma (optionnel)
- enforcement des types
- conversion de devises en USD (si les colonnes salary/currency/year existent)
- suppression des doublons

Appelée par toutes les variantes de `cleaning_pipeline`.
"""
function _clean(df::DataFrame;
    schema=nothing,
    schema_mode=:lenient,
    currency_col=:currency,
    salary_col=:salary,
    year_col=:year,
    do_standardize_colnames=true,
    do_enforce_types=true,
    do_currency_convert=true,
    do_deduplicate=true,
    keep_duplicates=:first,
    protected_cols=Symbol[],
    debug=false
)
    df2 = copy(df)

    do_standardize_colnames && standardize_colnames!(df2)
    schema !== nothing       && validate_schema(df2, schema, schema_mode)
    do_enforce_types         && (df2 = enforce_types(df2))

    if do_currency_convert && all(x -> x ∈ names(df2), (currency_col, salary_col, year_col))
        convert_currency_to_usd!(df2; currency_col=currency_col, salary_col=salary_col, year_col=year_col)
    end

    do_deduplicate && (df2 = deduplicate_rows(df2; mode=keep_duplicates, blind_rows=protected_cols))

    return df2
end

"""
    cleaning_pipeline(df::DataFrame) -> DataFrame

Pipeline simple appliquant toutes les étapes par défaut.
"""

function cleaning_pipeline(df::DataFrame)
    _clean(df)
end

"""
    cleaning_pipeline(df::DataFrame, schema::Dict; mode=:lenient) -> DataFrame

Pipeline avec validation de schéma selon `schema`.
`mode` peut être : `:lenient` ou `:strict`.
"""
function cleaning_pipeline(df::DataFrame, schema::Dict; mode=:lenient)
    _clean(df; schema=schema, schema_mode=mode)
end

"""
    cleaning_pipeline(df::DataFrame; kwargs...) -> DataFrame

Pipeline flexible : permet de contrôler toutes les options :
- do_standardize_colnames
- do_enforce_types
- do_currency_convert
- do_deduplicate
- keep_duplicates
- protected_cols
- debug
"""

function cleaning_pipeline(df::DataFrame;
    schema=nothing,
    schema_mode=:lenient,
    currency_col=:currency,
    salary_col=:salary,
    year_col=:year,
    do_standardize_colnames=true,
    do_enforce_types=true,
    do_currency_convert=true,
    do_deduplicate=true,
    keep_duplicates=:first,
    protected_cols=Symbol[],
    debug=false
)
    _clean(df;
        schema=schema,
        schema_mode=schema_mode,
        currency_col=currency_col,
        salary_col=salary_col,
        year_col=year_col,
        do_standardize_colnames=do_standardize_colnames,
        do_enforce_types=do_enforce_types,
        do_currency_convert=do_currency_convert,
        do_deduplicate=do_deduplicate,
        keep_duplicates=keep_duplicates,
        protected_cols=protected_cols,
        debug=debug,
    )
end

end
