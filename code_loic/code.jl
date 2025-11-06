# load_raw_csv: thin, safe wrapper around csv.read

"""
load_raw_csv(path; delim=',', kwargs...) -> DataFrame

Charge un CSV brut dans un DataFrame.

- path: chemin vers le fichier CSV.
- delim: séparateur (par défaut "','").
- kwargs...: options passées à CSV.read (ex: 'ignorerepeated=true', 'missingstring=["","NA"]').

Lève un ArgumentError si le ficier n'existe pas.
"""

function read_raw_csv(path::AbstractString; delim = ',', kwargs...)
    isfile(path) || throw(ArgumentError("CSV file not found at: $path"))
    return CSV.read(path, DataFrame; delim = delim, kwargs)
end



# standardize_colnames: snake case, no special caracters, no spaces
"""
standardize_colnames()
"""