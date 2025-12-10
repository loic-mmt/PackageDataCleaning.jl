struct SalaryTbl
    df::DataFrame
    
    function SalaryTbl(df::AbstractDataFrame)
        required_cols = [
            :work_year, :experience_level, :employment_type, :job_title,
            :salary, :salary_currency, :salary_in_usd, :employee_residence,
            :remote_ratio, :company_location, :company_size]
        validate_schema(df, required_cols)
        new(df)
        
    end
end