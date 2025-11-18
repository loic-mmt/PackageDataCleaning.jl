using Test, PackageDataCleaning
using DataFrames
using CategoricalArrays

using Test
using DataFrames


@testset "validate_range avec SalaryTbl" begin
    @testset "Données valides" begin
        df = (work_year = [2023, 2023, 2024, 2024],
            experience_level = ["MI", "SE", "EN", "EX"],
            employment_type = ["FT", "FT", "CT", "FL"],
            job_title = ["Data Scientist", "ML Engineer", "Analyst", "Director"],
            salary = [50000, 75000, 45000, 120000],
            salary_currency = ["USD", "EUR", "GBP", "USD"],
            salary_in_usd = [50000, 82000, 58000, 120000],
            employee_residence = ["US", "FR", "UK", "US"],
            remote_ratio = [0, 50, 100, 25],
            company_location = ["US", "FR", "UK", "US"],
            company_size = ["M", "L", "S", "M"])
        salary_tbl = SalaryTbl(df)
        result = validate_range(salary_tbl)
        
        @test result isa DataFrame
        @test names(result) == [:variables, :valid_mask]
        @test nrow(result) == 6
        @test all(result.valid_mask)
    end
    
    @testset "Données invalides" begin
        df = (work_year = [2023, 2023, 2024],
            experience_level = ["MI", "XX", "EN"],
            employment_type = ["FT", "INVALID", "CT"],
            job_title = ["DS", "ML", "DA"],
            salary = [50000, -1000, 45000],
            salary_currency = ["USD", "EUR", "GBP"],
            salary_in_usd = [50000, 82000, 45000],
            employee_residence = ["US", "FR", "UK"],
            remote_ratio = [0, 150, 100],
            company_location = ["US", "FR", "UK"],
            company_size = ["M", "XL", "S"])

        salary_tbl = SalaryTbl(df)
        result = validate_range(salary_tbl)
        
        @test result isa DataFrame
        @test !all(result.valid_mask)  # Au moins un test doit échouer
        
        # Vérification des échecs spécifiques
        valid_status = Dict(result.variables .=> result.valid_mask)
        @test valid_status["employment_type"] == false
        @test valid_status["experience_level"] == false
        @test valid_status["salary"] == false
        @test valid_status["remote_ratio"] == false
        @test valid_status["company_size"] == false
        @test valid_status["salary_in_usd"] == true  # Celui-ci devrait être valide
    end
    
    @testset "Données avec valeurs manquantes" begin
        df = (work_year = [2023, 2023, 2024],
            experience_level = ["MI", missing, "EN"],
            employment_type = ["FT", "PT", missing],
            job_title = ["DS", "ML", "DA"],
            salary = [50000, missing, 45000],
            salary_currency = ["USD", "EUR", "GBP"],
            salary_in_usd = [50000, 82000, 45000],
            employee_residence = ["US", "FR", "UK"],
            remote_ratio = [0, 50, missing],
            company_location = ["US", "FR", "UK"],
            company_size = ["M", "L", "S"])

        salary_tbl = SalaryTbl(df)
        result = validate_range(salary_tbl)
        
        @test result isa DataFrame
        # Les valeurs manquantes devraient être ignorées, donc les tests devraient passer
        @test all(result.valid_mask)
    end
end

@testset "validate_range avec DataFrame personnalisé" begin
    @testset "Tests basiques" begin
        df =(work_year = [2023, 2023, 2024, 2024],
            experience_level = ["MI", "SE", "EN", "EX"],
            employment_type = ["FT", "FT", "CT", "FL"],
            job_title = ["Data Scientist", "ML Engineer", "Analyst", "Director"],
            salary = [50000, 75000, 45000, 120000],
            salary_currency = ["USD", "EUR", "GBP", "USD"],
            salary_in_usd = [50000, 82000, 58000, 120000],
            employee_residence = ["US", "FR", "UK", "US"],
            remote_ratio = [0, 50, 100, 25],
            company_location = ["US", "FR", "UK", "US"],
            company_size = ["M", "L", "S", "M"])
        variables = [:salary, :salary_in_usd, :remote_ratio]
        tests = [
            x -> x > 0,
            x -> x < 200000,
            x -> 0 <= x <= 100
        ]
        
        result = validate_range(df, variables, tests)
        
        @test result isa DataFrame
        @test result.variables == variables
        @test all(result.valid_mask)
    end
    
    @testset "Tests avec échecs" begin
        df = (work_year = [2023, 2023, 2024, 2024],
            experience_level = ["MI", "SE", "EN", "EX"],
            employment_type = ["FT", "FT", "CT", "FL"],
            job_title = ["Data Scientist", "ML Engineer", "Analyst", "Director"],
            salary = [50000, 75000, 45000, 120000],
            salary_currency = ["USD", "EUR", "GBP", "USD"],
            salary_in_usd = [50000, 82000, 58000, 120000],
            employee_residence = ["US", "FR", "UK", "US"],
            remote_ratio = [0, 50, 100, 25],
            company_location = ["US", "FR", "UK", "US"],
            company_size = ["M", "L", "S", "M"])

        variables = [:salary, :remote_ratio]
        tests = [
            x -> x > 100000,
            x -> x == 100
        ]
        
        result = validate_range(df, variables, tests)
        
        @test all(.!result.valid_mask)
    end
    
    @testset "Vecteurs de longueurs différentes" begin
        df = (work_year = [2023, 2023, 2024, 2024],
            experience_level = ["MI", "SE", "EN", "EX"],
            employment_type = ["FT", "FT", "CT", "FL"],
            job_title = ["Data Scientist", "ML Engineer", "Analyst", "Director"],
            salary = [50000, 75000, 45000, 120000],
            salary_currency = ["USD", "EUR", "GBP", "USD"],
            salary_in_usd = [50000, 82000, 58000, 120000],
            employee_residence = ["US", "FR", "UK", "US"],
            remote_ratio = [0, 50, 100, 25],
            company_location = ["US", "FR", "UK", "US"],
            company_size = ["M", "L", "S", "M"])
        
        variables = [:salary, :salary_in_usd]
        tests = [x -> x > 0]
        
        @test_throws BoundsError validate_range(df, variables, tests)
    end
end

@testset "validate_range avec vecteur individuel" begin
    @testset "Vecteur numérique" begin
        data = [1, 2, 3, 4, 5]
        test_func = x -> x > 0
        
        result = validate_range(data, test_func)
        
        @test result isa Vector
        @test length(result) == 2
        @test result[1] == data
        @test result[2] == true
    end
    
    @testset "Vecteur avec échecs" begin
        data = [1, -2, 3, 4, 5]
        test_func = x -> x > 0
        
        result = validate_range(data, test_func)
        
        @test result[2] == false
    end
    
    @testset "Vecteur avec valeurs manquantes" begin
        data = [1, 2, missing, 4, 5]
        test_func = x -> x > 0
        
        result = validate_range(data, test_func)
        
        @test result[2] == true
    end
    
    @testset "Vecteur de strings" begin
        data = ["FT", "PT", "CT", "FL"]
        test_func = x -> x in EMPLOYMENT_TYPES
        
        result = validate_range(data, test_func)
        
        @test result[2] == true
    end
    
    @testset "Vecteur vide" begin
        data = Int[]
        test_func = x -> x > 0
        
        result = validate_range(data, test_func)
        
        @test result[1] == data
        @test result[2] == true
    end
end