using Test, PackageDataCleaning
using DataFrames
using Statistics
using CategoricalArrays

using Test
using DataFrames

#Ajout des constantes utilisées dans ces tests.
# test validate_range

@testset "validate_range avec SalaryTbl" begin
    @testset "Données valides" begin
        data = DataFrame(work_year = [2023, 2023, 2024, 2024],
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
        salary_tbl = SalaryTbl(data)
        result = validate_range(salary_tbl)
        
        @test result isa DataFrame
        @test names(result) == ["variables", "valid_mask"]
        @test nrow(result) == 6
        @test all(result.valid_mask)
    end
    
    @testset "Données invalides" begin
        data = DataFrame(work_year = [2023, 2023, 2024],
            experience_level = ["MI", "XX", "EN"],
            employment_type = ["FT", "INVALID", "CT"],
            job_title = ["DS", "ML", "DA"],
            salary = [50000, -1000, 45000],
            salary_currency = ["USD", "EUR", "GBP"],
            salary_in_usd = [50000, 82000, -45000],
            employee_residence = ["US", "FR", "UK"],
            remote_ratio = [0, 150, 100],
            company_location = ["US", "FR", "UK"],
            company_size = ["M", "L", "S"])

        salary_tbl = SalaryTbl(data)
        result = validate_range(salary_tbl)
        
        @test result isa DataFrame
        @test !all(result.valid_mask)
    
        valid_status = Dict(result.variables .=> result.valid_mask)
        @test (result.valid_mask == [false, false,false, false,false, true])
    end
    
    @testset "Données avec valeurs manquantes" begin
        data = DataFrame(work_year = [2023, 2023, 2024],
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

        salary_tbl = SalaryTbl(data)
        result = validate_range(salary_tbl)
        
        @test result isa DataFrame
        @test all(result.valid_mask)
    end
end

@testset "validate_range avec DataFrame personnalisé" begin
    @testset "Tests basiques" begin
        data = DataFrame(work_year = [2023, 2023, 2024, 2024],
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
        variables = ["salary", "salary_in_usd", "remote_ratio"]
        tests = [
            x -> x > 0,
            x -> x < 200000,
            x -> 0 <= x <= 100]
        
        result = validate_range(data, variables, tests)
        
        @test result isa DataFrame
        @test result.variables == variables
        @test all(result.valid_mask)
    end
    
    @testset "Tests avec échecs" begin
        data = DataFrame(work_year = [2023, 2023, 2024, 2024],
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
        
        result = validate_range(data, variables, tests)
        
        @test all(.!result.valid_mask)
    end
    
    @testset "Vecteurs de longueurs différentes" begin
        data = DataFrame(work_year = [2023, 2023, 2024, 2024],
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
        
        @test_throws BoundsError validate_range(data, variables, tests)
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
        test_func = x -> x in EMPLOYMENT_TYPES_FIXED
        
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

# tests winsorize

@testset "winsorize sur un vecteur" begin
    vect = [1, 2, 3, 100, 200]
    lower = quantile(vect, 0.05)
    upper = quantile(vect, 0.95)
    win = winsorize(vect; lower_quantile=0.05, upper_quantile=0.95)

    @test length(win) == length(vect)
    @test all(lower .<= win ) & all( win .<= upper)
    @test win[2] == vect[2]
    @test win[3] == vect[3]
    @test win[1] ≈ lower
    @test win[5] ≈ upper
end


@testset "winsorize sur un DataFrame" begin
    data = DataFrame(
        a = [-10, 1, 2, 3, 50],
        b = [100, 200, 300, 400, 500],
        c = ["x", "y", "z", "w", "k"])

    lower_a = quantile(data.a, 0.05)
    upper_a = quantile(data.a, 0.95)
    lower_b = quantile(data.b, 0.05)
    upper_b = quantile(data.b, 0.95)

    win = winsorize(data; lower_quantile=0.05, upper_quantile=0.95)

    @test all(lower_a .<= win.a .<= upper_a)
    @test all(lower_b .<= win.b .<= upper_b)
    @test (win.c == data.c)
    @test (size(data) == size(data))
end