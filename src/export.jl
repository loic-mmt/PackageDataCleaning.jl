# export_cleaned_csv

using CSV
using DataFrames

"""
    export_cleaned(path::AbstractString, df::AbstractDataFrame; delim = ',', kwargs...)

Export the cleaned DataFrame `df` to a CSV file at the location `path`.

`delim` controls the column delimiter (`,` by default). Any additional keyword
arguments in `kwargs` are forwarded to `CSV.write`.
"""
function export_cleaned(path::AbstractString, df::AbstractDataFrame; delim = ',', kwargs...)
    return CSV.write(path, df; delim = delim, kwargs...)
end

"""
    export_cleaned(io::IO, df::AbstractDataFrame; delim = ',', kwargs...)

Export the cleaned DataFrame `df` as CSV to an open IO stream `io`.

`delim` controls the column delimiter (`,` by default). Any additional keyword
arguments in `kwargs` are forwarded to `CSV.write`.
"""
function export_cleaned(io::IO, df::AbstractDataFrame; delim = ',', kwargs...)
    return CSV.write(io, df; delim = delim, kwargs...)
end