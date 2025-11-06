# test read_raw_csv
using Test, PackageDataCleaning

@test_throws ArgumentError load_raw_csv("fichier_qui_existe_pas.csv")

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
