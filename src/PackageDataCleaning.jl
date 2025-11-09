module PackageDataCleaning

using CSV, DataFrames, CategoricalArrays
include("validation.jl")
include("mappings.jl")
include("normalization.jl")
include("currency.jl")

export load_raw_csv,
       validate_schema,
       standardize_colnames!,
       enforce_types,
       deduplicate_rows,
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
       convert_currency_to_usd
    
end