# Tests for load_raw_csv
using Test, PackageDataCleaning
using DataFrames

@test_throws ArgumentError load_raw_csv("fichier_qui_existe_pas.csv")
@testset "load_raw_csv basic" begin
    # CSV temporaire
    csv_path = joinpath(@__DIR__, "sample_load_raw_csv.csv")
    open(csv_path, "w") do io
        write(io, "col1,col2\n1,hello\n2,world\n")
    end

    df = load_raw_csv(csv_path)

    @test size(df) == (2, 2)
    @test names(df) == [:col1, :col2]
    @test df.col1 == [1, 2]
    @test df.col2 == ["hello", "world"]
end


#test validate_schema
@testset "validate_schema" begin
    df = DataFrame(a = [1], b = [2])
    @test validate_schema(df, [:a, :b]) == true
    @test_throws ArgumentError validate_schema(df, [:a, :b, :c])
    missing = validate_schema(df, [:a, :b, :c]; strict=false)
    @test missing == [:c]
    @test validate_schema(df, (:a, :b)) == true
end


#test standardize_colnames!
@testset "standardize_colnames!" begin
    df = DataFrame("  My Col (1) " => [1,2], "SALAIRE (€)" => [10,20])
    standardize_colnames!(df)
    @test names(df) == [:my_col_1, :salaire]
end



#test d'enforce_type

@testset "enforce_types basic tests" begin
    df = DataFrame(
        a = ["1", "2", "3", "x", missing],
        b = ["chat", "chien", "chat", "souris", "chien"],
        c = ["", " ", "4", "5", "6"]
    )

    df2 = enforce_types(df)

    # Vérifie que a est numérique
    @test eltype(df2.a) <: Union{Missing, Int}

    # Vérifie que b est catégorielle
    @test isa(df2.b, CategoricalVector)

    # Vérifie que c est numérique (car majoritairement nombres)
    @test eltype(df2.c) <: Union{Missing, Float64}
end
