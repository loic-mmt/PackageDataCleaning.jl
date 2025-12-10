using Test
using DataFrames
using CSV
using PackageDataCleaning
using CategoricalArrays


@testset "MinimalPipeline" begin
    df = DataFrame(
        work_year = [2020, 2021],
        salary    = [100.0, 200.0],
    )

    df2 = PackageDataCleaning.pipeline(df, MinimalPipeline())

    @test isa(df2, DataFrame)
    @test nrow(df2) == 2
    # On vérifie que le nombre de lignes et de colonnes est conservé
    @test nrow(df2) == nrow(df)
    @test ncol(df2) == ncol(df)
end


@testset "LightCleanPipeline - imputation & modes" begin
    df = DataFrame(
        x_num  = [1.0, missing, 3.0],
        x_str  = ["a", missing, "b"],
        x_bool = [true, missing, false],
    )

    df2 = PackageDataCleaning.pipeline(df, LightCleanPipeline();
                   num_method  = NumMean(),
                   cat_method  = CatConstant("z"),
                   bool_method = BoolMajority())

    @test isa(df2, DataFrame)
    @test nrow(df2) == 3

    # NumMean : (1 + 3) / 2 = 2.0
    @test df2.x_num == [1.0, 2.0, 3.0]

    # CatConstant : remplace les missing par "z"
    @test df2.x_str == ["a", "z", "b"]

    # BoolMajority : en cas d'égalité, on s'attend à true (cf. tests unitaires dédiés)
    @test df2.x_bool == [true, true, false]
end


@testset "Dédoublonnage dans les pipelines (KeepFirst vs DropAll)" begin
    df = DataFrame(
        id = [1, 1, 2, 3, 3],
        x  = [10, 10, 20, 30, 30],
    )

    # LightCleanPipeline avec KeepFirst : on garde un exemplaire par groupe dupliqué
    df_keep = PackageDataCleaning.pipeline(df, LightCleanPipeline();
                       dedup_mode = KeepFirst(),
                       # méthodes d'imputation par défaut (pas de missing ici)
                       )

    @test isa(df_keep, DataFrame)
    @test nrow(df_keep) == 3   # {1,2,3}
    @test sort(df_keep.id) == [1, 2, 3]

    # NoImputePipeline avec DropAll : on supprime tous les doublons
    df_drop = PackageDataCleaning.pipeline(df, NoImputePipeline();
                       dedup_mode = DropAll())

    @test isa(df_drop, DataFrame)
    @test nrow(df_drop) == 1
    @test df_drop.id == [2]
end


@testset "StrictCleanPipeline - imputation stricte (CatNewLevel)" begin
    df = DataFrame(
        x_str = ["a", missing, "b", "c"],
    )

    df2 = PackageDataCleaning.pipeline(df, StrictCleanPipeline())

    @test isa(df2, DataFrame)
    @test nrow(df2) == 4

    # Plus aucun missing dans x_str
    @test all(.!ismissing.(df2.x_str))

    # Le nouveau niveau "NA" doit apparaître dans les valeurs
    @test "NA" in unique(df2.x_str)
end


@testset "MLReadyPipeline - mode company_size_order" begin
    # On crée deux DataFrames indépendants pour éviter les effets de mutation
    df_up = DataFrame(
        company_size = ["S", "M", "L"],
    )
    df_down = deepcopy(df_up)

    # On désactive la partie devise (pas de colonnes de salaire ici)
    res_up = PackageDataCleaning.pipeline(df_up, MLReadyPipeline();
                      company_size_order = UptoDown(),
                      do_currency = false)

    res_down = PackageDataCleaning.pipeline(df_down, MLReadyPipeline();
                        company_size_order = DowntoUp(),
                        do_currency = false)

    @test isa(res_up, DataFrame)
    @test isa(res_down, DataFrame)
    @test nrow(res_up) == 3
    @test nrow(res_down) == 3

    # On vérifie que la normalisation a produit une colonne catégorielle cohérente
    @test res_up.company_size isa CategoricalVector
    @test res_down.company_size isa CategoricalVector
    @test all(lev -> lev in levels(res_up.company_size), ["S", "M", "L"])
    @test all(lev -> lev in levels(res_down.company_size), ["S", "M", "L"])end


@testset "CurrencyFocusPipeline - conversion sans erreur" begin
    # Petit DataFrame minimal avec une structure plausible pour la conversion
    df = DataFrame(
        salary           = [1000.0, 2000.0],
        salary_currency  = ["USD", "EUR"],
        work_year        = [2020, 2021],
    )

    df2 = PackageDataCleaning.pipeline(df, CurrencyFocusPipeline())

    @test isa(df2, DataFrame)
    @test nrow(df2) == 2
    # On vérifie simplement que la colonne salary (et la colonne convertie) existent toujours
    colnames = String.(names(df2))
    @test "salary" in colnames
    @test "salary_in_usd" in colnames
end


@testset "export_pipeline - depuis un chemin" begin
    tmpdir = mktempdir()
    in_path  = joinpath(tmpdir, "raw.csv")
    out_path = joinpath(tmpdir, "clean.csv")

    df = DataFrame(
        x_num  = [1.0, missing, 3.0],
        x_str  = ["a", missing, "a"],
        x_bool = [true, missing, false],
    )

    # On écrit un CSV brut avec CSV.write pour tester la chaîne complète
    CSV.write(in_path, df)

    df_clean = PackageDataCleaning.export_pipeline(in_path, LightCleanPipeline(), out_path)

    @test isa(df_clean, DataFrame)
    @test isfile(out_path)

    # On relit le fichier exporté et on vérifie la cohérence
    df_out = DataFrame(CSV.File(out_path))
    @test df_out == df_clean
end


@testset "export_pipeline - depuis un DataFrame en mémoire" begin
    tmpdir = mktempdir()
    out_path = joinpath(tmpdir, "clean_from_df.csv")

    df = DataFrame(
        x_num  = [1.0, missing, 3.0],
        x_str  = ["a", missing, "b"],
        x_bool = [true, missing, false],
    )

    df_clean = PackageDataCleaning.export_pipeline(df, LightCleanPipeline(), out_path)

    @test isa(df_clean, DataFrame)
    @test isfile(out_path)

    df_out = DataFrame(CSV.File(out_path))
    @test df_out == df_clean
end