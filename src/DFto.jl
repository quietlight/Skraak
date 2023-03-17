# DFto.jl

module DFto

"""
DFto Submodules:
    audiodata_db

"""

export audiodata_db

using DataFrames, DBInterface, DuckDB, Random
using DSP, Plots, WAV, DataFrames, CSV, Glob

"""
audiodata_db(df::DataFrame, table::String)

Takes a dataframe and inserts into AudioData.db table.

using DataFrames, DBInterface, DuckDB, Random
"""

function audiodata_db(df::DataFrame, table::String)
    temp_name = randstring(6)
    con = DBInterface.connect(DuckDB.DB, "/media/david/USB/AudioData.db")
    #con = DBInterface.connect(DuckDB.DB, "/Users/davidcary/Desktop/AudioData.db")
    DuckDB.register_data_frame(con, df, temp_name)
    DBInterface.execute(
        con,
        """
        INSERT
        INTO $table
        SELECT *
        FROM '$temp_name'
        """,
    )
    DBInterface.close!(con)
end

#=
make dataset for image model

using CSV, DataFrames, DataFramesMeta, Glob

m = DataFrame(CSV.File("/media/david/USB/images_model/P_Male.csv"))

function get_td(drive, location, file)
       a=glob("$drive/$drive/$location/*/$file")
       length(a) > 0 ? b=split(a[1], "/")[end-1] : b=missing
       return b
       end

@transform!(m, @byrow :trip_date=get_td(:drive, :location, :file))

CSV.write("/media/david/USB/P_Male.csv", m)

f = DataFrame(CSV.File("/media/david/USB/images_model/P_Female.csv"))
@transform!(f, @byrow :trip_date=get_td(:drive, :location, :file))
CSV.write("/media/david/USB/P_Female.csv", f)


d = DataFrame(CSV.File("/media/david/USB/images_model/P_Duet.csv"))
@transform!(d, @byrow :trip_date=get_td(:drive, :location, :file))
CSV.write("/media/david/USB/P_Duet.csv", d)


files=glob("*.csv")
dfs = DataFrame.(CSV.File.(files))
df = reduce(vcat, dfs)
x=eval.(Meta.parse.(df.box)) 
df.box = x
sort!(df)
CSV.write("/media/david/USB/Aggregate.csv", df)

df2=df[4421:end, :]
=#

"""
img_dataset(df::DataFrame)

Takes a dataframe and makes png spectro images for secondary classifier.
Should be run from /media/david

using DSP, Plots, WAV, DataFrames, CSV, Glob
"""
function img_dataset(df::DataFrame)
    for row in eachrow(df)
        signal, freq = wavread("$(row.drive)/$(row.drive)/$(row.location)/$(row.trip_date)/$(row.file)")
        row.box[1] * freq > 1 ? st = floor(Int, (row.box[1] * freq)) : st = 1
        row.box[2] * freq < length(signal) ? en = ceil(Int, (row.box[2] * freq)) : en = length(signal)
        sample = signal[Int(st):Int(en)]
        name = "$(row.location)-$(row.trip_date)-$(chop(row.file, tail=4))-$(Int(floor(row.box[1])))-$(Int(ceil(row.box[2])))"
        outfile = "/home/david/ImageSet/$(row.label)/$name"
        #spectrogram
        n = 400
        fs = convert(Int, freq)
        S = spectrogram(sample[:, 1], n, n ÷ 200; fs = fs)
        heatmap(
            S.time,
            S.freq,
            pow2db.(S.power),
            xguide = "Time [s]",
            yguide = "Frequency [Hz]",
        )
        savefig(outfile)
        print(".")
    end
    println("done")
end


end #module
