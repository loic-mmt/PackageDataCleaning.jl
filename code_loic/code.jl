# normalize_experience_level, normalize_employment_type,
# normalize_company_size, normalize_remote_ratio,
# normalize_job_title, normalize_country_codes

import DataFrames: AbstractDataFrame, nrow, names
using CategoricalArrays


abstract type NormalMode end
struct UptoDown  <: NormalMode end
struct DowntoUp  <: NormalMode end

abstract type NormalizeField end
struct EmploymentType <: NormalizeField end
struct CompanySize    <: NormalizeField end
struct RemoteRatio    <: NormalizeField end
struct JobTitle       <: NormalizeField end
struct CountryCode    <: NormalizeField end

# Generic API
"""
    normalize!(df, field::NormalizeField, args...; kwargs...)

Normalise `df` sur place en fonction du type de `field`:

- `EmploymentType` : normalisation des types de contrat.
- `CompanySize`    : normalisation de la taille d'entreprise (ordre paramétrable).
- `RemoteRatio`    : projection vers un ensemble discret de valeurs.
- `JobTitle`       : harmonisation des intitulés de poste via dictionnaire.
- `CountryCode`    : harmonisation des pays vers des codes ISO2 (+ région optionnelle).

Le détail du comportement est entièrement déterminé par le multiple dispatch:
chaque combinaison de types a sa propre méthode spécialisée.
"""
function normalize!(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...)
    throw(ArgumentError("No matching normalization method for $(typeof(field)) with given arguments"))
end

"""
    normalize(df, field::NormalizeField, args...; kwargs...) -> DataFrame

Version non mutante de `normalize!`:
crée une copie de `df`, applique `normalize!` dessus et la renvoie.
"""
function normalize(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...)
    df2 = copy(df)
    normalize!(df2, field, args...; kwargs...)
    return df2
end

# EmploymentType

"""
    normalize!(df, EmploymentType(); col = :employment_type)

Mappe les codes:
- FT -> "Full-time"
- PT -> "Part-time"
- CT -> "Contract"
- FL -> "Freelance"

et convertit la colonne en `CategoricalArray` (non ordonnée).

Les valeurs non reconnues sont conservées telles quelles (pas écrasées),
pour ne rien perdre par rapport aux données brutes.
"""
function normalize!(df::AbstractDataFrame, ::EmploymentType; col::Symbol = :employment_type)
    if !(col in names(df))
        throw(ArgumentError("Column $(col) not found for EmploymentType normalization"))
    end

    df[!, col] = CategoricalArray(
        [v === missing ? missing :
         get(EMPLOYMENT_TYPE_MAPPING, String(v), String(v))
         for v in df[!, col]];
        ordered = false,
    )
    return df
end

# CompanySize

"""
    normalize!(df, CompanySize(), UptoDown(); col = :company_size)

Crée un facteur ordonné avec niveaux du plus petit au plus grand (S < M < L).

Les valeurs sont traitées comme des codes "S", "M", "L" déjà présentes dans le dataset.
"""
function normalize!(df::AbstractDataFrame, ::CompanySize, ::UptoDown; col::Symbol = :company_size)
    if !(col in names(df))
        throw(ArgumentError("Column $(col) not found for CompanySize normalization"))
    end

    raw = string.(df[!, col])
    levels = ["S", "M", "L"]
    df[!, col] = CategoricalArray(raw; levels = levels, ordered = true)
    return df
end

"""
    normalize!(df, CompanySize(), DowntoUp(); col = :company_size)

Crée un facteur ordonné avec niveaux du plus grand au plus petit (L < M < S).
"""
function normalize!(df::AbstractDataFrame, ::CompanySize, ::DowntoUp; col::Symbol = :company_size)
    if !(col in names(df))
        throw(ArgumentError("Column $(col) not found for CompanySize normalization"))
    end

    raw = string.(df[!, col])
    levels = ["L", "M", "S"]
    df[!, col] = CategoricalArray(raw; levels = levels, ordered = true)
    return df
end


# RemoteRatio

"""
    normalize!(df, RemoteRatio(); col=:remote_ratio, allowed=(0,50,100))

Force les valeurs de `col` dans l'ensemble `allowed` en prenant la valeur la plus proche.
`missing` est conservé. Si la colonne n'existe pas, lève une erreur.

Ex:
- 20 -> 0
- 40 -> 50
- 80 -> 100
"""
function normalize!(df::AbstractDataFrame, ::RemoteRatio;
                    col::Symbol = :remote_ratio,
                    allowed = (0, 50, 100))
    if !(col in names(df))
        throw(ArgumentError("Column $(col) not found for RemoteRatio normalization"))
    end

    vals = df[!, col]
    allowed_float = Float64.(allowed)

    df[!, col] = map(vals) do v
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

"""
    normalize!(df, JobTitle(); col=:job_title, mapping=JOB_TITLE_MAPPING)

Harmonise les intitulés de poste via un dictionnaire de correspondance.

- `col`     : nom de la colonne à normaliser.
- `mapping` : dictionnaire de correspondance brut -> catégorie canonique,
              par défaut `JOB_TITLE_MAPPING` défini dans `mappings.jl`.

Les valeurs non trouvées dans le mapping sont conservées telles quelles
(pour ne pas perdre d'information), mais la structure permet de plugger
des tables plus riches sans changer le code.
"""
function normalize!(df::AbstractDataFrame, ::JobTitle;
                    col::Symbol = :job_title,
                    mapping::AbstractDict{<:AbstractString,<:AbstractString} = JOB_TITLE_MAPPING)
    if !(col in names(df))
        throw(ArgumentError("Column $(col) not found for JobTitle normalization"))
    end

    df[!, col] = map(df[!, col]) do v
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

"""
    normalize!(df, CountryCode(); col=:country,
               mapping=COUNTRY_CODE_MAPPING,
               region_col::Union{Symbol,Nothing}=nothing)

Convertit les valeurs de `col` en codes ISO2 cohérents à l'aide d'un dictionnaire externe.

- `col`        : colonne contenant le pays / code brut.
- `mapping`    : dictionnaire brut -> ISO2 (par défaut `COUNTRY_CODE_MAPPING`).
- `region_col` :
    * `nothing` (défaut) : aucune colonne région ajoutée.
    * sinon : nom de la colonne où écrire la région, dérivée du code ISO2 via `REGION_MAP`.

Comportement:
- Si une valeur est trouvée dans `mapping` (en exact, upper ou lower), on utilise ce code.
- Sinon, on conserve la valeur originale comme code (aucune perte d'information).
- Si `region_col` est fourni, on crée/remplit cette colonne avec la région correspondante
  quand `REGION_MAP` contient le code, sinon `missing`.

Ce design montre bien le multiple dispatch: le choix de cette logique se fait
sur le type `CountryCode`, sans `if` globaux sur des strings.
"""
function normalize!(df::AbstractDataFrame, ::CountryCode;
                    col::Symbol = :country,
                    mapping = COUNTRY_CODE_MAPPING,
                    region_col::Union{Symbol,Nothing} = nothing)
    if !(col in names(df))
        throw(ArgumentError("Column $(col) not found for CountryCode normalization"))
    end

    n = nrow(df)
    codes = Vector{Union{Missing,String}}(undef, n)
    regions = region_col === nothing ? nothing : Vector{Union{Missing,String}}(undef, n)

    for (i, v) in pairs(df[!, col])
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

            code::Union{Missing,String}
            if m === nothing
                # Pas trouvé: on garde la valeur d'origine
                code = s
            elseif m isa String
                code = m
            elseif m isa NamedTuple
                # Support optionnel si un mapping plus riche fournit (:code, :region)
                code = haskey(m, :code) ? String(m.code) : s
            else
                code = String(m)
            end

            codes[i] = code

            if regions !== nothing
                regions[i] = (code !== missing && haskey(REGION_MAP, code)) ?
                             REGION_MAP[code] : missing
            end
        end
    end

    df[!, col] = codes
    if region_col !== nothing
        df[!, region_col] = regions
    end

    return df
end



#############################
# TESTS NORMALISATION
#############################

@testset "normalize (non-mutating wrapper)" begin
    df = DataFrame(employment_type = ["FT", "PT"])
    df2 = normalize(df, EmploymentType())
    @test df2 !== df
    @test df.employment_type[1] == "FT"              # original not modified
    if haskey(EMPLOYMENT_TYPE_MAPPING, "FT")
        @test df2.employment_type[1] == EMPLOYMENT_TYPE_MAPPING["FT"]
    end
end

@testset "normalize! EmploymentType" begin
    df = DataFrame(employment_type = ["FT", "PT", "CT", "FL", "XX", missing])
    normalize!(df, EmploymentType())

    @test df.employment_type isa CategoricalArray
    @test isequal(df.employment_type[6], missing)    # missing conservé
    @test df.employment_type[5] == "XX"              # valeur inconnue conservée telle quelle

    if haskey(EMPLOYMENT_TYPE_MAPPING, "FT")
        @test df.employment_type[1] == EMPLOYMENT_TYPE_MAPPING["FT"]
    end
    if haskey(EMPLOYMENT_TYPE_MAPPING, "PT")
        @test df.employment_type[2] == EMPLOYMENT_TYPE_MAPPING["PT"]
    end
end

@testset "normalize! EmploymentType - colonne manquante" begin
    df = DataFrame(other_col = [1, 2, 3])
    @test_throws ArgumentError normalize!(df, EmploymentType())
end

@testset "normalize! CompanySize UptoDown" begin
    df = DataFrame(company_size = ["S", "M", "L", "M"])
    normalize!(df, CompanySize(), UptoDown())

    @test df.company_size isa CategoricalArray
    @test isordered(df.company_size)
    @test levels(df.company_size) == ["S", "M", "L"]
end

@testset "normalize! CompanySize DowntoUp" begin
    df = DataFrame(company_size = ["S", "M", "L", "S"])
    normalize!(df, CompanySize(), DowntoUp())

    @test df.company_size isa CategoricalArray
    @test isordered(df.company_size)
    @test levels(df.company_size) == ["L", "M", "S"]
end

@testset "normalize! CompanySize - colonne manquante" begin
    df = DataFrame(other_col = [1])
    @test_throws ArgumentError normalize!(df, CompanySize(), UptoDown())
    @test_throws ArgumentError normalize!(df, CompanySize(), DowntoUp())
end

@testset "normalize! RemoteRatio (par défaut allowed = (0,50,100))" begin
    df = DataFrame(remote_ratio = [20, 40, 80, 0, 50, 100, missing])
    normalize!(df, RemoteRatio())

    @test df.remote_ratio == [0, 50, 100, 0, 50, 100, missing]
end

@testset "normalize! RemoteRatio - colonne manquante" begin
    df = DataFrame(other_col = [1])
    @test_throws ArgumentError normalize!(df, RemoteRatio())
end

@testset "normalize! JobTitle - mapping et fallback" begin
    # 1) Test générique basé sur JOB_TITLE_MAPPING si non vide
    if !isempty(JOB_TITLE_MAPPING)
        raw, canon = first(collect(JOB_TITLE_MAPPING))
        df = DataFrame(job_title = [raw, lowercase(raw), "Unknown title", missing])
        normalize!(df, JobTitle())

        # clé exacte ou via lowercase doit renvoyer la forme canonique
        @test df.job_title[1] == canon || df.job_title[1] == get(JOB_TITLE_MAPPING, raw, raw)
        @test df.job_title[2] == canon || df.job_title[2] == get(JOB_TITLE_MAPPING, lowercase(raw), lowercase(raw))

        # valeur inconnue conservée
        @test df.job_title[3] == "Unknown title"
        # missing conservé
        @test isequal(df.job_title[4], missing)
    else
        # 2) Si JOB_TITLE_MAPPING est vide, on teste les comportements génériques
        df = DataFrame(job_title = ["Some title", missing])
        normalize!(df, JobTitle())
        @test df.job_title[1] == "Some title"
        @test isequal(df.job_title[2], missing)
    end
end

@testset "normalize! JobTitle - colonne manquante" begin
    df = DataFrame(other_col = [1])
    @test_throws ArgumentError normalize!(df, JobTitle())
end

@testset "normalize! CountryCode - sans région" begin
    df = DataFrame(country = ["US", "fr", "UnknownLand", missing])
    normalize!(df, CountryCode())

    # missing conservé
    @test isequal(df.country[4], missing)
    # valeur inconnue conservée
    @test df.country[3] == "UnknownLand"
end

@testset "normalize! CountryCode - avec région" begin
    df = DataFrame(country = ["US", "FR", "UnknownLand", missing])
    normalize!(df, CountryCode(); region_col = :region)

    @test :region in names(df)
    @test nrow(df) == length(df.region)
    @test isequal(df.region[4], missing)

    # Si REGION_MAP contient un code connu, on vérifie la cohérence
    if df.country[1] !== missing && haskey(REGION_MAP, df.country[1])
        @test df.region[1] == REGION_MAP[df.country[1]]
    end
    if df.country[2] !== missing && haskey(REGION_MAP, df.country[2])
        @test df.region[2] == REGION_MAP[df.country[2]]
    end
    # UnknownLand doit produire missing en région
    @test isequal(df.region[3], missing)
end

@testset "normalize! CountryCode - colonne manquante" begin
    df = DataFrame(other_col = [1])
    @test_throws ArgumentError normalize!(df, CountryCode())
end
