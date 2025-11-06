# load_raw_csv, validate_schema, standardize_colnames, enforce_types, deduplicate_rows

# load_raw_csv: thin, safe wrapper around csv.read
"""
load_raw_csv(path; delim=',', kwargs...) -> DataFrame

Charge un CSV brut dans un DataFrame.

- path: chemin vers le fichier CSV.
- delim: séparateur (par défaut "','").
- kwargs...: options passées à CSV.read (ex: 'ignorerepeated=true', 'missingstring=["","NA"]').

Lève un ArgumentError si le ficier n'existe pas.
"""

function load_raw_csv(path::AbstractString; delim = ',', kwargs...)
    isfile(path) || throw(ArgumentError("CSV file not found at: $path"))
    return CSV.read(path, DataFrame; delim = delim, kwargs)
end