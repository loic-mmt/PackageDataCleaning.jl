module PackageDataCleaning

import DataFrames: AbstractDataFrame, nrow, names
using DataFrames
using CategoricalArrays
using CSV

include("validation.jl")
include("mappings.jl")
include("utils.jl")
include("normalization.jl")
include("currency.jl")


export load_raw_csv,
       validate_schema,
       standardize_colnames!,
       enforce_types,
       deduplicate_rows,
       _resolve_col,
       DropAll,
       KeepFirst,
       normalize,
       normalize!,
       EmploymentType,
       CompanySize,
       UptoDown,
       DowntoUp,
       RemoteRatio,
       JobTitle,
       CountryCode,
       UseExchangeRates,
       convert_currency_to_usd!,
       convert_currency_to_usd,
       CurrencyConversionMode
    
end