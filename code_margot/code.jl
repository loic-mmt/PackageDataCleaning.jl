using DataFrames, CategoricalArrays

function enforce_types(df::DataFrame; num_threshold=0.9, max_factor_levels=20)
    out = copy(df)
    for col in names(out)
        x = out[!, col]

        if eltype(x) <: Union{Number, CategoricalValue}
            continue
        end

        xs = [ismissing(v) ? missing : strip(String(v)) for v in x]
        valid = filter(v -> !ismissing(v) && v != "", xs)
        n_valid = length(valid)
        if n_valid == 0
            continue
        end

        parsed = tryparse.(Float64, xs)
        n_numeric_valid = count(!ismissing, parsed)

        if n_numeric_valid / n_valid >= num_threshold
            nums = Float64.(coalesce.(parsed, NaN))
            if all(ismissing(v) || isinteger(v) for v in parsed)
                out[!, col] = convert(Vector{Union{Missing, Int}}, round.(Int, nums))
            else
                out[!, col] = convert(Vector{Union{Missing, Float64}}, nums)
            end
            continue
        end

        n_unique = length(unique(valid))
        if n_unique <= max_factor_levels
            out[!, col] = categorical(xs)
        else
            out[!, col] = xs
        end
    end
    return out
end
