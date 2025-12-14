using Test
using CSV
using DataFrames
using PackageDataCleaning

@testset "export_cleaned" begin
    df = DataFrame(
        a = [1, 2],
        b = ["hello", "x;y"],
        c = [missing, "ok"]
    )

    @testset "writes to a file path" begin
        mktemp() do path, io
            close(io)

            PackageDataCleaning.export_cleaned(path, df)

            df_read = DataFrame(CSV.File(path))
            @test isequal(df_read, df)
        end
    end

    @testset "writes to IO stream" begin
        io = IOBuffer()
        PackageDataCleaning.export_cleaned(io, df)

        seekstart(io)
        df_read = DataFrame(CSV.File(io))
        @test isequal(df_read, df)
    end

    @testset "respects custom delimiter" begin
        io = IOBuffer()
        PackageDataCleaning.export_cleaned(io, df; delim=';')

        # vérifie le contenu
        s = String(take!(io))
        lines = split(chomp(s), '\n')
        @test occursin("a;b;c", lines[1])  # header avec ';'
    end

    @testset "forwards kwargs to CSV.write (writeheader=false)" begin
        io = IOBuffer()
        PackageDataCleaning.export_cleaned(io, df; writeheader=false)

        s = String(take!(io))
        first_line = split(chomp(s), '\n')[1]

        # sans header, la première ligne doit commencer par des données
        @test startswith(first_line, "1,")
        @test !occursin("a,b,c", first_line)
    end
end