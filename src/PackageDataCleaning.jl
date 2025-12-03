module PackageDataCleaning

import DataFrames: AbstractDataFrame, nrow, names, leftjoin!, select!, Not, rename!
using DataFrames
using Statistics: mean, median
using CategoricalArrays
using CSV

include("validation.jl")
include("mappings.jl")
include("utils.jl")
include("normalization.jl")
include("currency.jl")
include("missing.jl")
include("export.jl")


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
       CurrencyConversionMode,
       ImputeMethod,
       NumericImputeMethod,
       NumMedian,
       NumMean,
       NumConstant,
       CategoricalImputeMethod,
       CatMode,
       CatConstant,
       CatNewLevel,
       BoolImputeMethod,
       BoolMajority,
       impute_missing!,
       impute_missing,
       impute_column!
       export_cleaned

end