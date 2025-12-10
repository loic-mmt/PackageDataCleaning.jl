using PackageDataCleaning
using Test
using DataFrames

@testset "PackageDataCleaning.jl" begin
    include("test_normalization.jl")
    include("test_validation.jl")
    include("test_currency.jl")
    include("test_quality_outliers.jl")
    include("test_missing.jl")
    include("test_pipeline.jl")

end

