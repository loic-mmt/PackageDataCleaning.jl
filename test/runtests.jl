using PackageDataCleaning
using Test
using DataFrames

@testset "PackageDataCleaning.jl" begin
    include("test_normalization.jl")
    include("test_validation.jl")
    include("test_currency.jl")

end

using Test
using PackageDataCleaning
using DataFrames

