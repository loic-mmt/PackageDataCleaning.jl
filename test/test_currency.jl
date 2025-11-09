using Test
using PackageDataCleaning
using DataFrames

@testset "convert_currency_to_usd (non-mutating wrapper)" begin
    df = DataFrame(
        salary = [50000, 60000],
        salary_currency = ["EUR", "USD"],
        work_year = [2022, 2022]
    )
    df2 = convert_currency_to_usd(df, UseExchangeRates())
    @test df2 !== df
    @test !any(n -> n == :salary_in_usd || n == "salary_in_usd", names(df))  # original non modifié
    @test any(n -> n == :salary_in_usd || n == "salary_in_usd", names(df2))  # copie contient la colonne
end

@testset "convert_currency_to_usd! UseExchangeRates" begin
    df = DataFrame(
        salary = [50000, 60000, 70000, missing],
        salary_currency = ["EUR", "GBP", "USD", "CAD"],
        work_year = [2022, 2022, 2023, 2021]
    )

    convert_currency_to_usd!(df, UseExchangeRates())

    @test any(n -> n == :salary_in_usd || n == "salary_in_usd", names(df))
    @test nrow(df) == 4

    # USD doit rester USD (rate = 1.0)
    @test df.salary_in_usd[3] ≈ 70000.0

    # missing doit être propagé
    @test isequal(df.salary_in_usd[4], missing)

    # EUR et GBP doivent être convertis (on vérifie juste qu'ils ne sont pas égaux au salaire original)
    @test df.salary_in_usd[1] != df.salary[1]
    @test df.salary_in_usd[2] != df.salary[2]
end

@testset "convert_currency_to_usd! UseExchangeRates - colonne manquante" begin
    df = DataFrame(salary = [50000])
    @test_throws ArgumentError convert_currency_to_usd!(df, UseExchangeRates())

    df = DataFrame(salary_currency = ["EUR"])
    @test_throws ArgumentError convert_currency_to_usd!(df, UseExchangeRates())

    df = DataFrame(work_year = [2022])
    @test_throws ArgumentError convert_currency_to_usd!(df, UseExchangeRates())
end

@testset "convert_currency_to_usd! UseExchangeRates - devises inconnues" begin
    df = DataFrame(
        salary = [50000, 60000],
        salary_currency = ["EUR", "XYZ"],  # XYZ n'existe pas
        work_year = [2022, 2022]
    )

    convert_currency_to_usd!(df, UseExchangeRates())

    # EUR doit être converti
    @test df.salary_in_usd[1] isa Float64

    # XYZ doit produire missing (pas de taux trouvé)
    @test isequal(df.salary_in_usd[2], missing)
end

@testset "convert_currency_to_usd! UseExchangeRates - années multiples" begin
    df = DataFrame(
        salary = [50000, 50000, 50000],
        salary_currency = ["EUR", "EUR", "EUR"],
        work_year = [2020, 2022, 2023]
    )

    convert_currency_to_usd!(df, UseExchangeRates())

    # Les taux varient par année, donc les résultats doivent être différents
    @test df.salary_in_usd[1] != df.salary_in_usd[2] ||
          df.salary_in_usd[2] != df.salary_in_usd[3]
end

@testset "convert_currency_to_usd! UseExchangeRates - colonnes personnalisées" begin
    df = DataFrame(
        mon_salaire = [50000],
        ma_devise = ["EUR"],
        annee = [2022]
    )

    convert_currency_to_usd!(df, UseExchangeRates();
                             salary_col = :mon_salaire,
                             currency_col = :ma_devise,
                             year_col = :annee,
                             usd_col = :salaire_usd)

    @test any(n -> n == :salaire_usd || n == "salaire_usd", names(df))
    @test df.salaire_usd[1] isa Float64
end

@testset "convert_currency_to_usd! UseExchangeRates - exchange_rates personnalisés" begin
    custom_rates = DataFrame(
        year = [2022, 2022],
        currency = ["EUR", "GBP"],
        rate = [2.0, 3.0]  # taux fantaisistes pour le test
    )

    df = DataFrame(
        salary = [100, 100],
        salary_currency = ["EUR", "GBP"],
        work_year = [2022, 2022]
    )

    convert_currency_to_usd!(df, UseExchangeRates(); exchange_rates = custom_rates)

    @test df.salary_in_usd[1] ≈ 200.0
    @test df.salary_in_usd[2] ≈ 300.0
end

@testset "convert_currency_to_usd! - pas de méthode pour type inconnu" begin
    struct UnknownMode <: CurrencyConversionMode end

    df = DataFrame(
        salary = [50000],
        salary_currency = ["EUR"],
        work_year = [2022]
    )

    @test_throws ArgumentError convert_currency_to_usd!(df, UnknownMode())
end

@testset "convert_currency_to_usd! UseExchangeRates - missing dans salary" begin
    df = DataFrame(
        salary = [50000, missing, 70000],
        salary_currency = ["EUR", "GBP", "USD"],
        work_year = [2022, 2022, 2023]
    )

    convert_currency_to_usd!(df, UseExchangeRates())

    @test df.salary_in_usd[1] isa Float64
    @test isequal(df.salary_in_usd[2], missing)
    @test df.salary_in_usd[3] isa Float64
end

@testset "convert_currency_to_usd! UseExchangeRates - missing dans currency" begin
    df = DataFrame(
        salary = [50000, 60000, 70000],
        salary_currency = ["EUR", missing, "USD"],
        work_year = [2022, 2022, 2023]
    )

    convert_currency_to_usd!(df, UseExchangeRates())

    @test df.salary_in_usd[1] isa Float64
    @test isequal(df.salary_in_usd[2], missing)
    @test df.salary_in_usd[3] isa Float64
end