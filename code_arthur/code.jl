using DataFrames


"""
    SalaryTbl

Un type pour représenter les DataFrames proches de ds_salaries
"""
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

"""
    finalize_salary_tbl(data::AbstractDataFrame) -> SalaryTbl

Crée un objet SalaryTbl à partir d'un DataFrame après validation du schéma.

# Arguments
- `data` : Un DataFrame contenant les données salariales

# Retour
- Un objet de type `SalaryTbl`

# Exemple
```julia
df = DataFrame(work_year=2023, experience_level="Senior", ...)
salary_data = finalize_salary_tbl(df) """








function validate_range end


function validate_range(data::SalaryTbl)
    valid_mask = []
    push!(valid_mask, all(x -> x in EMPLOYMENT_TYPES, skipmissing(data[!, employment_type])))
    push!(valid_mask, all(x -> x in EXPERIENCE, skipmissing(data[!, experience_level])))
    push!(valid_mask, all(x -> x >0, skipmissing(data[!, salary])))
    push!(valid_mask, all(x -> x >0, skipmissing(data[!, salary_in_usd])))
    push!(valid_mask, all(x -> 0 <= x && x <= 100, skipmissing(data[!, remote_ratio])))
    push!(valid_mask, all(x -> x in SIZE, skipmissing(data[!, company_size])))
    return valid_mask
end

function validate_range(data::DataFrame, vars_a_tester::AbstractVector, tests_a_effectuer::AbstractVector)
    valid_mask = []
    for index in range length(vars_a_tester)
        push!(valid_mask, all(tests_a_effectuer[index], skipmissing(data[!, vars_a_tester[index]])))
    end
    return(valid_mask)
end

function validate_range(var::AbstractVector, test::AbstractVector)
    return(all(test, skipmissing(var)))
end



function winsorize end

function winsorize(vect::AbstractVector; lower_quantile=0.05, upper_quantile=0.95)
    lower = quantile(vect, lower_quantile)
    upper = quantile(vect, upper_quantile)
    return max(min(vect, upper), lower)
end

function winsorize(data::AbstractDataFrame; lower_quantile=0.05, upper_quantile=0.95)
    lower = quantile(vect, lower_quantile)
    upper = quantile(vect, upper_quantile)
    for col_name in names(data)
        col = data[!, col_name]
        if eltype(col) <: Real && length(col) > 1
            data[!, col_name] <- max(min(col, upper), lower)
        end
    end
    return(data)
end