# deduplicate_rows

"""
    deduplicate_rows(df, mode::DedupMode; by=names(df),
                     blind_rows = Int[],
                     blind_col::Union{Symbol,Nothing} = nothing,
                     blind_values = nothing) -> DataFrame

Supprime ou conserve les doublons selon le mode choisi.

- `df` : DataFrame d'entrée (non muté).
- `mode` : stratégie typée :
    - `KeepFirst()` : conserve la première occurrence, supprime les doublons suivants.
    - `DropAll()`   : supprime **toutes** les lignes appartenant à un groupe dupliqué (ne garde que les lignes uniques).
- `by` : colonnes utilisées pour définir l'égalité entre lignes (par défaut toutes les colonnes).
- `blind_rows` : indices de lignes à ne jamais supprimer (protégées).
- `blind_col` / `blind_values` :
    - si spécifiés, toute ligne dont `df[!, blind_col]` est dans `blind_values` est protégée.
"""

abstract type DedupMode end
struct KeepFirst <: DedupMode end      # garde la première occurrence
struct DropAll   <: DedupMode end      # ne garde que les clés (valeurs) apparaissant une seule fois


# construit la clé de déduplication pour une ligne
@inline _dedup_key(df::AbstractDataFrame, i::Int, by) =
    ntuple(j -> df[i, by[j]], length(by))

# indique si une ligne est "protégée" (jamais supprimée)
function _is_protected(df::AbstractDataFrame, i::Int,
                       blind_rows::AbstractVector{Int},
                       blind_col::Union{Symbol,Nothing},
                       blind_values)
    if i in blind_rows
        return true
    end
    if blind_col !== nothing && blind_values !== nothing
        v = df[i, blind_col]
        return v in blind_values
    end
    return false
end

"Mode KeepFirst: conserve la 1ère occurrence, supprime les doublons suivants (sauf lignes protégées)."
function deduplicate_rows(df::AbstractDataFrame, ::KeepFirst;
                          by = names(df),
                          blind_rows::AbstractVector{Int} = Int[],
                          blind_col::Union{Symbol,Nothing} = nothing,
                          blind_values = nothing)

    by_syms = Symbol.(by)
    seen = Set{Tuple}()
    keep = trues(nrow(df))

    for i in 1:nrow(df)
        if _is_protected(df, i, blind_rows, blind_col, blind_values)
            # Protégée: on la garde, mais elle compte dans les "seen"
            key = _dedup_key(df, i, by_syms)
            push!(seen, key)
            continue
        end

        key = _dedup_key(df, i, by_syms)
        if key in seen
            keep[i] = false
        else
            push!(seen, key)
        end
    end

    return df[keep, :]
end

"Mode DropAll: ne garde que les lignes dont la clé n'apparaît qu'une seule fois (sauf lignes protégées)."
function deduplicate_rows(df::AbstractDataFrame, ::DropAll;
                          by = names(df),
                          blind_rows::AbstractVector{Int} = Int[],
                          blind_col::Union{Symbol,Nothing} = nothing,
                          blind_values = nothing)

    by_syms = Symbol.(by)

    # Compter le nombre d'occurrences de chaque clé
    counts = Dict{Tuple,Int}()
    for i in 1:nrow(df)
        key = _dedup_key(df, i, by_syms)
        counts[key] = get(counts, key, 0) + 1
    end

    keep = trues(nrow(df))
    for i in 1:nrow(df)
        if _is_protected(df, i, blind_rows, blind_col, blind_values)
            continue
        end
        key = _dedup_key(df, i, by_syms)
        if counts[key] > 1
            keep[i] = false
        end
    end

    return df[keep, :]
end

@testset "deduplicate_rows DropAll basic" begin
    df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
                   b = ["a", "b", "b", "c", "d", "d", "e"])

    # On déduplique par la colonne :a uniquement
    out = deduplicate_rows(df, DropAll(); by = [:a])

    @test size(out) == (2, 2)
    @test all(out.a .== [2, 4])  # seules les valeurs uniques 2 et 4
end

@testset "deduplicate_rows KeepFirst basic" begin
    df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
                   b = ["a", "b", "b", "c", "d", "d", "e"])

    out = deduplicate_rows(df, KeepFirst(); by = [:a])

    @test size(out) == (4, 2)
    @test out.a == [1, 2, 3, 4]
end

@testset "deduplicate_rows DropAll with blind_rows" begin
    df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
                   b = ["a", "b", "b", "c", "d", "d", "e"])

    out = deduplicate_rows(df, DropAll(); by = [:a], blind_rows = [1])

    @test out.a == [1, 2, 4]
end

@testset "deduplicate_rows DropAll with blind_values" begin
    df = DataFrame(a = [1, 1, 2, 3, 3, 3, 4],
                   b = ["a", "b", "b", "c", "d", "d", "e"])

    out = deduplicate_rows(df, DropAll(); by = [:a], blind_col = :a, blind_values = [3])

    @test sort(out.a) == [2, 3, 3, 3, 4]
end