# petites fonctions transverses (normalisation de chaînes, mappings, etc.)
"""
    _resolve_col(df::AbstractDataFrame, col::Symbol)

Résout le nom d'une colonne dans un DataFrame, en gérant les différences de classe ou de type.
Retourne le nom exact de la colonne tel qu'il apparaît dans le DataFrame.

# Arguments
- `df::AbstractDataFrame` : Le DataFrame dans lequel chercher
- `col::Symbol` : Le nom de la colonne à résoudre

# Throws
- `ArgumentError` : Si la colonne n'est pas trouvée dans le DataFrame
"""

function _resolve_col(df::AbstractDataFrame, col::Symbol)
    for name in names(df)
        if name == col || String(name) == String(col)
            return name
        end
    end
    throw(ArgumentError("Column $(col) not found"))
end