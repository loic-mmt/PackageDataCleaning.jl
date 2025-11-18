# normalize_experience_level, normalize_employment_type,
# normalize_company_size, normalize_remote_ratio,
# normalize_job_title, normalize_country_codes



"""
    NormalMode

Type abstrait pour représenter un mode d’ordonnancement dans certaines
normalisations (par ex. `CompanySize()`).
"""
abstract type NormalMode end

"""
    UptoDown <: NormalMode

Mode d’ordonnancement "du plus petit au plus grand" (ex: `S < M < L`).
Utilisé avec `CompanySize()` dans `normalize!`.
"""
struct UptoDown  <: NormalMode end

"""
    DowntoUp <: NormalMode

Mode d’ordonnancement "du plus grand au plus petit" (ex: `L < M < S`).
Utilisé avec `CompanySize()` dans `normalize!`.
"""
struct DowntoUp  <: NormalMode end

"""
    NormalizeField

Type abstrait pour représenter un "champ métier" normalisable
(type de contrat, taille d’entreprise, pays, etc.).
"""
abstract type NormalizeField end

"""
    EmploymentType <: NormalizeField

Champ métier pour normaliser une colonne de type de contrat
(ex: codes `FT`, `PT`, `CT`, `FL`).
"""
struct EmploymentType <: NormalizeField end

"""
    CompanySize <: NormalizeField

Champ métier pour normaliser une colonne de taille d’entreprise
(ex: `"S"`, `"M"`, `"L"`).
"""
struct CompanySize    <: NormalizeField end

"""
    RemoteRatio <: NormalizeField

Champ métier pour normaliser un ratio de télétravail en valeurs discrètes
(ex: `0`, `50`, `100`).
"""
struct RemoteRatio    <: NormalizeField end

"""
    JobTitle <: NormalizeField

Champ métier pour harmoniser les intitulés de poste via un dictionnaire
de correspondance.
"""
struct JobTitle       <: NormalizeField end

"""
    CountryCode <: NormalizeField

Champ métier pour convertir des pays / codes bruts en codes ISO2 cohérents
(et éventuellement une région).
"""
struct CountryCode    <: NormalizeField end


"""
    normalize!(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...)
    normalize!(df, EmploymentType(); col = :employment_type)
    normalize!(df, CompanySize(), UptoDown(); col = :company_size)
    normalize!(df, CompanySize(), DowntoUp(); col = :company_size)
    normalize!(df, RemoteRatio(); col = :remote_ratio, allowed = (0, 50, 100))
    normalize!(df, JobTitle(); col = :job_title, mapping = JOB_TITLE_MAPPING)
    normalize!(df, CountryCode(); col = :country,
               mapping = COUNTRY_CODE_MAPPING,
               region_col::Union{Symbol,Nothing} = nothing)

Applique une opération de normalisation en place sur `df` en fonction du type
de champ métier (`field`) et, éventuellement, d'un mode supplémentaire.

Cette fonction est le point d’entrée générique : pour chaque combinaison de `field`
et d’arguments (`args...` / `kwargs...`), une méthode spécialisée de `normalize!`
est sélectionnée par multiple dispatch.

# Variantes supportées

- `normalize!(df, EmploymentType(); col = :employment_type)`  
  Normalise une colonne de type de contrat. Les codes suivants sont mappés :  
  `FT -> "Full-time"`, `PT -> "Part-time"`, `CT -> "Contract"`, `FL -> "Freelance"`.  
  La colonne est convertie en `CategoricalArray` non ordonnée.  
  Les valeurs non reconnues sont conservées telles quelles.

- `normalize!(df, CompanySize(), UptoDown(); col = :company_size)`  
  Crée un facteur ordonné représentant la taille d’entreprise du plus petit au
  plus grand (`S < M < L`). La colonne est traitée comme composée de codes `"S"`,
  `"M"`, `"L"` et convertie en `CategoricalArray{String}` ordonnée avec les niveaux
  `["S", "M", "L"]`.

- `normalize!(df, CompanySize(), DowntoUp(); col = :company_size)`  
  Crée un facteur ordonné représentant la taille d’entreprise du plus grand au
  plus petit (`L < M < S`) avec les niveaux `["L", "M", "S"]`.

- `normalize!(df, RemoteRatio(); col = :remote_ratio, allowed = (0, 50, 100))`  
  Projette les valeurs d’un ratio de télétravail vers l’ensemble discret `allowed`
  en prenant la valeur la plus proche. Par défaut : `(0, 50, 100)`.  
  Les valeurs `missing` sont conservées.

- `normalize!(df, JobTitle(); col = :job_title, mapping = JOB_TITLE_MAPPING)`  
  Harmonise les intitulés de poste via un dictionnaire de correspondance
  `brut -> catégorie canonique`. La logique essaie la clé exacte, puis en minuscules
  si besoin. Les valeurs non trouvées dans le mapping sont conservées.

- `normalize!(df, CountryCode(); col = :country,
               mapping = COUNTRY_CODE_MAPPING,
               region_col = nothing)`  
  Convertit des pays / codes bruts en codes ISO2 cohérents à l’aide d’un dictionnaire
  externe. Si `region_col` est fourni, une colonne supplémentaire est remplie avec
  la région correspondante via `REGION_MAP`. Lorsque le mapping échoue, la valeur
  brute est conservée pour éviter de perdre de l’information.

# Arguments

- `df`    : `AbstractDataFrame` à normaliser (modifié en place).
- `field` : type de champ indiquant la normalisation à appliquer
            (`EmploymentType()`, `CompanySize()`, `RemoteRatio()`, `JobTitle()`,
             `CountryCode()`, etc.).
- `args...` / `kwargs...` : paramètres supplémentaires spécifiques à chaque type
  de normalisation (par exemple `col`, `allowed`, `mapping`, `region_col`, etc.).

# Retour

- Le même `DataFrame`, modifié en place.


# Exemples

Normalisation en place du type de contrat :

```julia
df = DataFrame(employment_type = ["FT", "PT", "CT", "FL", "XX", missing])
normalize!(df, EmploymentType())

df.employment_type isa CategoricalArray
isequal(df.employment_type[6], missing)    # missing conservé
df.employment_type[5] == "XX"              # valeur inconnue conservée telle quelle
```

Tailles d’entreprise ordonnées (du plus petit au plus grand, puis l’inverse) :

```julia
df = DataFrame(company_size = ["S", "M", "L", "M"])
normalize!(df, CompanySize(), UptoDown())
levels(df.company_size) == ["S", "M", "L"]
isordered(df.company_size) == true

df2 = DataFrame(company_size = ["S", "M", "L", "S"])
normalize!(df2, CompanySize(), DowntoUp())
levels(df2.company_size) == ["L", "M", "S"]
```

Projection d’un ratio de télétravail sur un ensemble discret :

```julia
df = DataFrame(remote_ratio = [20, 40, 80, 0, 50, 100, missing])
normalize!(df, RemoteRatio())  # allowed = (0, 50, 100) par défaut

isequal(df.remote_ratio, [0, 50, 100, 0, 50, 100, missing])
```

Normalisation des intitulés de poste :

```julia
df = DataFrame(job_title = ["Senior Data Scientist", "sr data scientist", "Unknown title", missing])
normalize!(df, JobTitle())

df.job_title[3] == "Unknown title"   # valeur inconnue conservée
isequal(df.job_title[4], missing)    # missing conservé
```

Normalisation des pays / codes et ajout d’une région :

```julia
df = DataFrame(country = ["US", "FR", "UnknownLand", missing])
normalize!(df, CountryCode(); region_col = :region)

any(n -> n == :region || n == "region", names(df))
nrow(df) == length(df.region)
isequal(df.region[4], missing)
isequal(df.region[3], missing)       # UnknownLand -> région manquante
```

# Notes

- Si aucune méthode spécialisée ne correspond à la combinaison de types fournie,
  un `ArgumentError` est levé pour indiquer qu’il n’existe pas de méthode adaptée.
- Voir également [`normalize`] pour la version non mutante qui travaille sur une copie.
# Notes
"""
function normalize!(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...)
    throw(ArgumentError("No matching normalization method for $(typeof(field)) with given arguments"))
end


"""
    normalize(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...) -> DataFrame

Version non mutante de [`normalize!`] : crée une copie de `df`, applique `normalize!`
sur cette copie, puis renvoie le résultat.

# Arguments

- `df`    : `AbstractDataFrame` d’entrée (non modifié).
- `field` : type de champ indiquant la normalisation à appliquer
            (`EmploymentType()`, `CompanySize()`, `RemoteRatio()`, `JobTitle()`, `CountryCode()`, etc.).
- `args...` / `kwargs...` : paramètres supplémentaires passés à la méthode `normalize!`
  correspondante.

# Retour

- Un nouveau `DataFrame` contenant la version normalisée de `df`.

# Notes

- Utile lorsque l’on souhaite chaîner plusieurs transformations sans muter les
  données sources.
"""
function normalize(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...)
    df2 = copy(df)
    normalize!(df2, field, args...; kwargs...)
    return df2
end

# EmploymentType

function normalize!(df::AbstractDataFrame, ::EmploymentType; col::Symbol = :employment_type)
    colname = _resolve_col(df, col)

    df[!, colname] = CategoricalArray(
        [v === missing ? missing :
         get(EMPLOYMENT_TYPE_MAPPING, String(v), String(v))
         for v in df[!, colname]];
        ordered = false,
    )
    return df
end

# CompanySize

function normalize!(df::AbstractDataFrame, ::CompanySize, ::UptoDown; col::Symbol = :company_size)
    colname = _resolve_col(df, col)

    raw = string.(df[!, colname])
    levels = ["S", "M", "L"]
    df[!, colname] = CategoricalArray(raw; levels = levels, ordered = true)
    return df
end

function normalize!(df::AbstractDataFrame, ::CompanySize, ::DowntoUp; col::Symbol = :company_size)
    colname = _resolve_col(df, col)

    raw = string.(df[!, colname])
    levels = ["L", "M", "S"]
    df[!, colname] = CategoricalArray(raw; levels = levels, ordered = true)
    return df
end


# RemoteRatio

function normalize!(df::AbstractDataFrame, ::RemoteRatio;
                    col::Symbol = :remote_ratio,
                    allowed = (0, 50, 100))
    colname = _resolve_col(df, col)

    vals = df[!, colname]
    allowed_float = Float64.(allowed)

    df[!, colname] = map(vals) do v
        if v === missing
            missing
        else
            x = Float64(v)
            allowed_float[argmin(abs.(allowed_float .- x))]
        end
    end

    return df
end


# JobTitle

function normalize!(df::AbstractDataFrame, ::JobTitle;
                    col::Symbol = :job_title,
                    mapping::AbstractDict{<:AbstractString,<:AbstractString} = JOB_TITLE_MAPPING)
    colname = _resolve_col(df, col)

    df[!, colname] = map(df[!, colname]) do v
        if v === missing
            missing
        else
            s = String(v)
            get(mapping, s, get(mapping, lowercase(s), s))
        end
    end

    return df
end

# CountryCode (+ région optionnelle)

function normalize!(df::AbstractDataFrame, ::CountryCode;
                    col::Symbol = :country,
                    mapping = COUNTRY_CODE_MAPPING,
                    region_col::Union{Symbol,Nothing} = nothing)
    colname = _resolve_col(df, col)

    n = nrow(df)
    codes = Vector{Union{Missing,String}}(undef, n)
    regions = region_col === nothing ? nothing : Vector{Union{Missing,String}}(undef, n)

    for (i, v) in pairs(df[!, colname])
        if v === missing
            codes[i] = missing
            if regions !== nothing
                regions[i] = missing
            end
        else
            s = String(v)

            # On essaie exact, upper, lower dans le mapping
            m = get(mapping, s,
                    get(mapping, uppercase(s),
                        get(mapping, lowercase(s), nothing)))

            code = if m === nothing
                # Pas trouvé: on garde la valeur d'origine
                s
            elseif m isa String
                m
            elseif m isa NamedTuple
                # Support optionnel si un mapping plus riche fournit (:code, :region)
                haskey(m, :code) ? String(m.code) : s
            else
                String(m)
            end

            codes[i] = code

            if regions !== nothing
                regions[i] = (code !== missing && haskey(REGION_MAP, code)) ?
                             REGION_MAP[code] : missing
            end
        end
    end

    df[!, colname] = codes
    if region_col !== nothing
        df[!, region_col] = regions
    end

    return df
end
