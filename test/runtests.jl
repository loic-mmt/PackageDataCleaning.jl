using PackageDataCleaning
using Test

@testset "PackageDataCleaning.jl" begin
    include("test_normalization.jl")
    include("test_validation.jl")

end
