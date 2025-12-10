using Test
using DataFrames
using PackageDataCleaning

@testset "SalaryTbl" begin
    valid_cols = [
        :work_year, :experience_level, :employment_type, :job_title,
        :salary, :salary_currency, :salary_in_usd, :employee_residence,
        :remote_ratio, :company_location, :company_size
    ]
    
    df_valid = DataFrame(work_year = [2023, 2023, 2024, 2024],
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
    
    @testset "Valid dataset" begin
        tbl = SalaryTbl(df_valid)
        println("DEBUG: Type of tbl is ", typeof(tbl))
        @test tbl isa SalaryTbl
        @test tbl.df == df_valid
    end

    @testset "Invalid dataset" begin
        df_invalid = DataFrame(
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
        @test_throws ArgumentError SalaryTbl(df_invalid)
    end
end