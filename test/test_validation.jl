# Tests for load_raw_csv
using Test, PackageDataCleaning
using DataFrames
using CategoricalArrays


@testset "load_raw_csv" begin
    @test_throws ArgumentError load_raw_csv("fichier_qui_existe_pas.csv")
    # CSV temporaire
    csv_path = joinpath(@__DIR__, "sample_load_raw_csv.csv")
    open(csv_path, "w") do io
        write(io, "col1,col2\n1,hello\n2,world\n")
    end

    df = load_raw_csv(csv_path)

    @test size(df) == (2, 2)
    @test names(df) == ["col1", "col2"]
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
    @test names(df) == ["my_col_1", "salaire"]
end



#test d'enforce_type

@testset "enforce_types basic tests" begin
    df = DataFrame(
        a = ["1", "2", "3", "x", missing],
        b = ["chat", "chien", "chat", "souris", "chien"],
        c = ["", " ", "4", "5", "6"]
    )

    df2 = enforce_types(df)

    # Colonne a : majorité numérique mais avec une valeur non numérique ("x") -> classée en catégorielle
    @test isa(df2.a, CategoricalVector)

    # Vérifie que b est catégorielle
    @test isa(df2.b, CategoricalVector)

    # Vérifie que c est numérique (car majoritairement nombres) : accepte Int ou Float64
    @test eltype(df2.c) <: Union{Missing, Int, Float64}
end


# deduplicate_rows
@testset "deduplicate_rows DropAll basic" begin
    df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
                   b = ["a", "b", "b", "c", "d", "d", "e"])

    # On déduplique par la colonne :a uniquement
    out = deduplicate_rows(df, DropAll(); by = [:a])

    @test size(out) == (2, 2)
    @test all(out.a .== [2, 4])  # seules les valeurs uniques 2 et 4
end

@testset "deduplicate_rows KeepFirst basic" begin
    df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
                   b = ["a", "b", "b", "c", "d", "d", "e"])

    out = deduplicate_rows(df, KeepFirst(); by = [:a])

    @test size(out) == (4, 2)
    @test out.a == [1, 2, 3, 4]
end

@testset "deduplicate_rows DropAll with blind_rows" begin
    df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
                   b = ["a", "b", "b", "c", "d", "d", "e"])

    out = deduplicate_rows(df, DropAll(); by = [:a], blind_rows = [1])

    @test out.a == [1, 2, 4]
end

@testset "deduplicate_rows DropAll with blind_values" begin
    df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
                   b = ["a", "b", "b", "c", "d", "d", "e"])

    out = deduplicate_rows(df, DropAll(); by = [:a], blind_col = :a, blind_values = [3])

    @test sort(out.a) == [2, 3, 3, 3, 4]
end