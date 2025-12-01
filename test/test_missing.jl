using Test
using DataFrames
using PackageDataCleaning
using CategoricalArrays

@testset "impute_missing test" begin
    df = DataFrame(
        x_num  = [1.0, missing, 3.0],
        x_str  = ["a", missing, "a"],
        x_bool = [true, missing, false]
    )
    expected = DataFrame(
        x_num  = [1.0, 2.0, 3.0],
        x_str  = ["a", "a", "a"],
        x_bool = [true, true, false]
    )
    impute_missing!(df)
    @test df == expected
end

@testset "impute_missing non mutant" begin
    df = DataFrame(
        x_num  = [1.0, missing, 3.0],
        x_str  = ["a", missing, "a"],
        x_bool = [true, missing, false]
    )

    df2 = impute_missing(df)

    @test df !== df2          # objets distincts
    @test any(ismissing, df.x_num)
    @test any(ismissing, df.x_str)
    @test any(ismissing, df.x_bool)
    @test df2 == DataFrame(
        x_num  = [1.0, 2.0, 3.0],
        x_str  = ["a", "a", "a"],
        x_bool = [true, true, false]
    )
end

@testset "impute_missing NumMean" begin
    df = DataFrame(x_num = [1.0, missing, 3.0])
    impute_missing!(df; num_method = NumMean())
    @test df.x_num == [1.0, 2.0, 3.0]
end


@testset "impute_missing : selection de colonnes (cols)" begin
    df = DataFrame(
        x_num  = [1.0, missing, 3.0],
        x_str  = ["a", missing, "b"],
        x_bool = [true, missing, false]
    )
    impute_missing!(df; cols = [:x_num, :x_bool])
    @test df.x_num == [1.0, 2.0, 3.0]      # imputé (NumMedian par défaut)
    @test df.x_bool == [true, true, false] # imputé (BoolMajority)
    @test any(ismissing, df.x_str)         # non imputé (hors cols)
end

@testset "impute_missing : exclusion (exclude)" begin
    df = DataFrame(
        x_num  = [1.0, missing, 3.0],
        x_str  = ["a", missing, "a"],
        x_bool = [true, missing, false]
    )
    impute_missing!(df; exclude = [:x_num])
    @test any(ismissing, df.x_num)         # exclu -> reste missing
    @test df.x_str == ["a", "a", "a"]      # imputé (CatMode)
    @test df.x_bool == [true, true, false] # imputé (BoolMajority)
end

@testset "impute_missing : NumConstant" begin
    df = DataFrame(x_num = [missing, 2.0, missing])
    impute_missing!(df; num_method = NumConstant(0.0))
    @test df.x_num == [0.0, 2.0, 0.0]
end

@testset "impute_missing : CatConstant (String)" begin
    df = DataFrame(x_str = ["a", missing, "b", missing])
    impute_missing!(df; cat_method = CatConstant("unknown"))
    @test df.x_str == ["a", "unknown", "b", "unknown"]
end

@testset "impute_missing : CatMode sur CategoricalArray" begin
    df = DataFrame(x = categorical(["a", missing, "a", "b"]))
    @test isa(df.x, CategoricalVector)
    impute_missing!(df; cat_method = CatMode())
    @test String.(df.x) == ["a", "a", "a", "b"]
    @test Set(levels(df.x)) == Set(["a","b"])
end

@testset "impute_missing : CatNewLevel sur CategoricalArray" begin
    df = DataFrame(x = categorical(["a", missing, "b", missing]))
    impute_missing!(df; cat_method = CatNewLevel("NA"))
    @test "NA" in levels(df.x)
    @test df.x == categorical(["a", "NA", "b", "NA"])
end

@testset "impute_missing : BoolMajority (égalité -> true)" begin
    df = DataFrame(x = [true, false, missing, missing])
    impute_missing!(df; bool_method = BoolMajority())
    @test df.x == [true, false, true, true]  # tie -> true
end

@testset "impute_missing : toutes valeurs missing (numérique)" begin
    df = DataFrame(x = Union{Missing,Float64}[missing, missing])
    impute_missing!(df)  # rien à imputer (pas de valeur observée)
    @test all(ismissing, df.x)
end

@testset "impute_missing : version non mutante avec options" begin
    df = DataFrame(
        x_num  = [1.0, missing, 3.0],
        x_str  = ["a", missing, "a"]
    )
    df2 = impute_missing(df; num_method = NumMean(), cat_method = CatConstant("x"))
    @test df !== df2
    @test df2.x_num == [1.0, 2.0, 3.0]
    @test df2.x_str == ["a", "x", "a"]
end

@testset "impute_missing : cols acceptant des String" begin
    df = DataFrame(
        :x_num => [1.0, missing, 3.0],
        :x_str => ["a", missing, "a"]
    )
    # cols en String -> doit être accepté
    impute_missing!(df; cols = ["x_num"])
    @test df.x_num == [1.0, 2.0, 3.0]
    @test any(ismissing, df.x_str)
end