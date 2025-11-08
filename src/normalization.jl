# normalize_experience_level, normalize_employment_type, normalize_company_size, normalize_remote_ratio, normalize_job_title, normalize_country_codes

abstract type NormalMode end
struct UptoDown  <: NormalMode end
struct DowntoUp  <: NormalMode end

abstract type NormalizeField end
struct EmploymentType <: NormalizeField end
struct CompanySize    <: NormalizeField end
struct RemoteRatio    <: NormalizeField end

"""
    normalize!(df, field::NormalizeField, args...; kwargs...)

Normalise `df` sur place selon:
- le type de `field` (EmploymentType, CompanySize, RemoteRatio),
- éventuellement un `NormalMode` (UptoDown, DowntoUp).
"""
function normalize!(df::AbstractDataFrame, field::NormalizeField, args...; kwargs...)
    # Documentation pour l'API générique.
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


"""
    normalize!(df, EmploymentType(); col = :employment_type)

Mappe les codes:
- FT -> "Full-time"
- PT -> "Part-time"
- CT -> "Contract"
- FL -> "Freelance"

et convertit la colonne en `CategoricalArray` (non ordonnée).
"""
function normalize!(df::AbstractDataFrame, ::EmploymentType; col::Symbol = :employment_type)
    if !haskey(df, col)
        throw(ArgumentError("Column $(col) not found for EmploymentType normalization"))
    end

    mapping = Dict(
        "FT" => "Full-time",
        "PT" => "Part-time",
        "CT" => "Contract",
        "FL" => "Freelance",
    )

    coldata = df[!, col]
    df[!, col] = CategoricalArray(
        [haskey(mapping, String(x)) ? mapping[String(x)] : x for x in coldata];
        ordered = false,
    )
    return df
end


"""
    normalize!(df, CompanySize(), UptoDown(); col = :company_size)

Crée un facteur ordonné avec niveaux du plus petit au plus grand (S < M < L).
"""
function normalize!(df::AbstractDataFrame, ::CompanySize, ::UptoDown; col::Symbol = :company_size)
    if !haskey(df, col)
        throw(ArgumentError("Column $(col) not found for CompanySize normalization"))
    end

    raw = string.(df[!, col])
    levels = ["S", "M", "L"]
    df[!, col] = CategoricalArray(raw; levels=levels, ordered=true)
    return df
end

"""
    normalize!(df, CompanySize(), DowntoUp(); col = :company_size)

Crée un facteur ordonné avec niveaux du plus grand au plus petit (L < M < S).
"""
function normalize!(df::AbstractDataFrame, ::CompanySize, ::DowntoUp; col::Symbol = :company_size)
    if !haskey(df, col)
        throw(ArgumentError("Column $(col) not found for CompanySize normalization"))
    end

    raw = string.(df[!, col])
    levels = ["L", "M", "S"]
    df[!, col] = CategoricalArray(raw; levels=levels, ordered=true)
    return df
end


"""
    normalize!(df, RemoteRatio(); col=:remote_ratio, allowed=(0,50,100))

Force les valeurs de `col` dans l'ensemble `allowed` en prenant la valeur la plus proche.
`missing` est conservé. Si la colonne n'existe pas, lève une erreur.
"""
function normalize!(df::AbstractDataFrame, ::RemoteRatio;
                    col::Symbol = :remote_ratio,
                    allowed = (0, 50, 100))
    if !haskey(df, col)
        throw(ArgumentError("Column $(col) not found for RemoteRatio normalization"))
    end

    vals = df[!, col]
    allowed_float = Float64.(allowed)

    df[!, col] = map(vals) do v
        if v === missing
            missing
        else
            x = Float64(v)
            # valeur de allowed la plus proche
            allowed_float[argmin(abs.(allowed_float .- x))]
        end
    end

    return df
end
