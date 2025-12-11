
"""
    struct SalaryTbl

Structure enveloppante pour un `DataFrame` contenant des données de salaire, garantissant
la présence d'un schéma de colonnes spécifique à la création.

# Champs

- `df::DataFrame` : Le DataFrame sous-jacent contenant les données.

# Constructeur

    SalaryTbl(df::AbstractDataFrame)

Crée une nouvelle instance de `SalaryTbl`. Ce constructeur effectue une validation
stricte du schéma (`validate_schema`) avant de créer l'objet.

Les colonnes suivantes **doivent** être présentes dans `df` :
- `:work_year`
- `:experience_level`
- `:employment_type`
- `:job_title`
- `:salary`
- `:salary_currency`
- `:salary_in_usd`
- `:employee_residence`
- `:remote_ratio`
- `:company_location`
- `:company_size`

# Exceptions

- Lève une erreur (via `validate_schema`) si une ou plusieurs colonnes requises sont manquantes.

# Exemples

```julia
df = DataFrame(
    work_year = [2023], experience_level = ["SE"], employment_type = ["FT"],
    job_title = ["Data Scientist"], salary = [100000], salary_currency = ["USD"],
    salary_in_usd = [100000], employee_residence = ["US"], remote_ratio = [0],
    company_location = ["US"], company_size = ["M"]
)

tbl = SalaryTbl(df)
# SalaryTbl(...)
```
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