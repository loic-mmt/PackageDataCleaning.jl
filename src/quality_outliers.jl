# validate_ranges, cap_outliers_salary
"""
A function for validation of conditions in variables

validate_range(data::SalaryTbl) -> DataFrame
Verify that in all the variables that can be verfied values are plausibles.
- 'data': le SalaryTbl à tester
- 'df': dataframe contenant le résulat du test (Booléen), pour chaque variable.
Permet de voir quelle variable contient l'erreur si il y en a une.

validate_range(data::DataFrame, vars_a_tester::AbstractVector, tests_a_effectuer::AbstractVector{Function}) -> DataFrame
Verify that in all the variables that can be verfied values are plausibles.
- 'data': le DataFrame à tester
- 'vars_a_tester': vecteur des variables à tester
- 'tests_a_effectuer': vecteur des tests à appliquer aux variables (fonctions qui renvoient un booléen)
- 'df': dataframe contenant le résulat du test (Booléen), pour chaque variable (char).
Permet de voir quelle variable contient l'erreur si il y en a une.

validate_range(var::AbstractVector, test::AbstractVector{Function})-> DataFrame
Verify that in all the variables that can be verfied values are plausibles.
- 'var': vecteur à tester
- 'test': fonction qui renvoie un booléen pour tester le vecteur
- 'verif': booléen value which indicate is the test as passed.
Permet de voir quelle variable contient l'erreur si il y en a une.
"""


function validate_range end


function validate_range(data::SalaryTbl)
    valid_mask = Bool[]
    push!(valid_mask, all(x -> x in EMPLOYMENT_TYPES, skipmissing(data.df[!, :employment_type])))
    push!(valid_mask, all(x -> x in EXPERIENCE, skipmissing(data.df[!, :experience_level])))
    push!(valid_mask, all(x -> x >0, skipmissing(data.df[!, :salary])))
    push!(valid_mask, all(x -> x >0, skipmissing(data.df[!, :salary_in_usd])))
    push!(valid_mask, all(x -> 0 <= x && x <= 100, skipmissing(data.df[!, :remote_ratio])))
    push!(valid_mask, all(x -> x in SIZE, skipmissing(data.df[!, :company_size])))
    df = DataFrame(variables = ["employment_type", "experience_level", "salary", "salary_in_usd", "remote_ratio", "company_size"], valid_mask = valid_mask)
    return df
end



function validate_range(data::DataFrame, vars_a_tester::AbstractVector, tests_a_effectuer::AbstractVector{<:Function})
    valid_mask = Bool[]
    for index in 1:length(vars_a_tester)
        push!(valid_mask, all(tests_a_effectuer[index], skipmissing(data[!, vars_a_tester[index]])))
    end
    df = DataFrame(variables = vars_a_tester, valid_mask = valid_mask)
    return df
end


function validate_range(var::AbstractVector, test::Function)
    verif = [var, all(test, skipmissing(var))]
    return verif
end



function winsorize end

function winsorize(vect::AbstractVector; lower_quantile=0.05, upper_quantile=0.95)
    lower = quantile(vect, lower_quantile)
    upper = quantile(vect, upper_quantile)
    return max.(min.(vect, upper), lower)
end

function winsorize(data::AbstractDataFrame; lower_quantile=0.05, upper_quantile=0.95)
    for col_name in names(data)
        col = data[!, col_name]
        if eltype(col) <: Real && length(col) > 1
            lower = quantile(col, lower_quantile)
            upper = quantile(col, upper_quantile)
            data[!, col_name] = max.(min.(col, upper), lower)
        end
    end
    return(data)
end