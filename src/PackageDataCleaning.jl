module PackageDataCleaning

using CSV, DataFrames, CategoricalArrays
include("validation.jl")
include("mappings.jl")
include("normalization.jl")

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
       CountryCode

end