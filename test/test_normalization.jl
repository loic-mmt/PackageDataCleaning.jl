using Test
using PackageDataCleaning
using DataFrames
using CategoricalArrays

@testset "normalize (non-mutating wrapper)" begin
    df = DataFrame(employment_type = ["FT", "PT"])
    df2 = normalize(df, EmploymentType())
    @test df2 !== df
    @test df.employment_type[1] == "FT"              # original not modified
    if haskey(PackageDataCleaning.EMPLOYMENT_TYPE_MAPPING, "FT")
        @test df2.employment_type[1] == PackageDataCleaning.EMPLOYMENT_TYPE_MAPPING["FT"]
    end
end

@testset "normalize! EmploymentType" begin
    df = DataFrame(employment_type = ["FT", "PT", "CT", "FL", "XX", missing])
    normalize!(df, EmploymentType())

    @test df.employment_type isa CategoricalArray
    @test isequal(df.employment_type[6], missing)    # missing conservé
    @test df.employment_type[5] == "XX"              # valeur inconnue conservée telle quelle

    if haskey(PackageDataCleaning.EMPLOYMENT_TYPE_MAPPING, "FT")
        @test df.employment_type[1] == PackageDataCleaning.EMPLOYMENT_TYPE_MAPPING["FT"]
    end
    if haskey(PackageDataCleaning.EMPLOYMENT_TYPE_MAPPING, "PT")
        @test df.employment_type[2] == PackageDataCleaning.EMPLOYMENT_TYPE_MAPPING["PT"]
    end
end

@testset "normalize! EmploymentType - colonne manquante" begin
    df = DataFrame(other_col = [1, 2, 3])
    @test_throws ArgumentError normalize!(df, EmploymentType())
end

@testset "normalize! CompanySize UptoDown" begin
    df = DataFrame(company_size = ["S", "M", "L", "M"])
    normalize!(df, CompanySize(), UptoDown())

    @test df.company_size isa CategoricalArray
    @test isordered(df.company_size)
    @test levels(df.company_size) == ["S", "M", "L"]
end

@testset "normalize! CompanySize DowntoUp" begin
    df = DataFrame(company_size = ["S", "M", "L", "S"])
    normalize!(df, CompanySize(), DowntoUp())

    @test df.company_size isa CategoricalArray
    @test isordered(df.company_size)
    @test levels(df.company_size) == ["L", "M", "S"]
end

@testset "normalize! CompanySize - colonne manquante" begin
    df = DataFrame(other_col = [1])
    @test_throws ArgumentError normalize!(df, CompanySize(), UptoDown())
    @test_throws ArgumentError normalize!(df, CompanySize(), DowntoUp())
end

@testset "normalize! RemoteRatio (par défaut allowed = (0,50,100))" begin
    df = DataFrame(remote_ratio = [20, 40, 80, 0, 50, 100, missing])
    normalize!(df, RemoteRatio())

    @test isequal(df.remote_ratio, [0, 50, 100, 0, 50, 100, missing])
end

@testset "normalize! RemoteRatio - colonne manquante" begin
    df = DataFrame(other_col = [1])
    @test_throws ArgumentError normalize!(df, RemoteRatio())
end

@testset "normalize! JobTitle - mapping et fallback" begin
    # 1) Test générique basé sur JOB_TITLE_MAPPING si non vide
    if !isempty(PackageDataCleaning.JOB_TITLE_MAPPING)
        raw, canon = first(collect(PackageDataCleaning.JOB_TITLE_MAPPING))
        df = DataFrame(job_title = [raw, lowercase(raw), "Unknown title", missing])
        normalize!(df, JobTitle())

        # clé exacte ou via lowercase doit renvoyer la forme canonique
        @test df.job_title[1] == canon || df.job_title[1] == get(PackageDataCleaning.JOB_TITLE_MAPPING, raw, raw)
        @test df.job_title[2] == canon || df.job_title[2] == get(PackageDataCleaning.JOB_TITLE_MAPPING, lowercase(raw), lowercase(raw))

        # valeur inconnue conservée
        @test df.job_title[3] == "Unknown title"
        # missing conservé
        @test isequal(df.job_title[4], missing)
    else
        # 2) Si JOB_TITLE_MAPPING est vide, on teste les comportements génériques
        df = DataFrame(job_title = ["Some title", missing])
        normalize!(df, JobTitle())
        @test df.job_title[1] == "Some title"
        @test isequal(df.job_title[2], missing)
    end
end

@testset "normalize! JobTitle - colonne manquante" begin
    df = DataFrame(other_col = [1])
    @test_throws ArgumentError normalize!(df, JobTitle())
end

@testset "normalize! CountryCode - sans région" begin
    df = DataFrame(country = ["US", "fr", "UnknownLand", missing])
    normalize!(df, CountryCode())

    # missing conservé
    @test isequal(df.country[4], missing)
    # valeur inconnue conservée
    @test df.country[3] == "UnknownLand"
end

@testset "normalize! CountryCode - avec région" begin
    df = DataFrame(country = ["US", "FR", "UnknownLand", missing])
    normalize!(df, CountryCode(); region_col = :region)

    @test any(n -> n == :region || n == "region", names(df))
    @test nrow(df) == length(df.region)
    @test isequal(df.region[4], missing)

    # Si REGION_MAP contient un code connu, on vérifie la cohérence
    if df.country[1] !== missing && haskey(PackageDataCleaning.REGION_MAP, df.country[1])
        @test df.region[1] == PackageDataCleaning.REGION_MAP[df.country[1]]
    end
    if df.country[2] !== missing && haskey(PackageDataCleaning.REGION_MAP, df.country[2])
        @test df.region[2] == PackageDataCleaning.REGION_MAP[df.country[2]]
    end
    # UnknownLand doit produire missing en région
    @test isequal(df.region[3], missing)
end

@testset "normalize! CountryCode - colonne manquante" begin
    df = DataFrame(other_col = [1])
    @test_throws ArgumentError normalize!(df, CountryCode())
end
