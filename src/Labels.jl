# Labels.jl

export aggregate_labels, audiodata_db

using CSV, DataFrames, Glob, Random, DBInterface, DuckDB
using DataFramesMeta: @transform!, @subset!, @byrow, @passmissing

#=
actual.csv must be list of qualified png file names:
D/C05-2023-04-15-20230219_223000-380-470.png

using Glob, DataFrames, CSV
a=glob("[M,F,D,N]/*.png")
df = DataFrame(file=a)
CSV.write("actual_mfdn.csv", df)

make a folder D,F,M,N
mkpath.(["D", "F", "M", "N"])

move wavs to match pngs
df=DataFrame(CSV.File("actual_mfdn.csv"))
for row in eachrow(df)
   src=split(row.file, "/")[2]
   dst=row.file
   mv(src, dst)
   mv(chop(src, tail=3)*"wav", chop(dst, tail=3)*"wav")
end
=#
"""
actual_from_folders(labels::Vector{String})::DataFrame

run from parent folder of label folders
saves actual.csv and returns a df
labels=["D", "F", "M", "N"]
"""
function actual_from_folders(labels::Vector{String})::DataFrame
    paths = String[]
    for l in labels
        paths = append!(paths, glob("$l/*.png"))
    end
    df = DataFrame(file = paths)
    CSV.write("actual.csv", df)
    return df
end

"""
aggregate_labels(actual="actual.csv", outfile="labels.csv")

file
[D, F, M, N]/C05-2023-04-15-20230219_223000-380-470.png

This function takes the csv output from my hand classification and ouputs a df, and csv for insertion into AudioData.duckdb using the duckdb cli or using DFto.audiodata_db()

assumes run from Clips_xxxx-xx-xx folder and that "actual.csv" present if not specified.
returns a dataframe
Note: need to add empty columns to get to O, use easyCSV

using CSV, DataFrames, DataFramesMeta
"""
#=
df=aggregate_labels()

audiodata_db(df, "pomona_labels_20230418") NOT_WORKING maybe titles
to use cli, need to remove header row

duckdb /media/david/SSD1/AudioData.duckdb
COPY pomona_labels_20230418 FROM 'DB_Labels/pomona_labels_2024-06-23.csv';
COPY pomona_files FROM 'DB_Files/pomona_files_20240623.csv';
COPY twenty_four_seven_files FROM 'DB_Files/twenty_four_seven_files_20240509.csv';

Then backup with:
EXPORT DATABASE 'AudioDataBackup_2024-07-10';
.quit
Then quit and backup using cp on the db file, dated copy

Then rsync ssd to usb
rsync -avzr  --delete /media/david/SSD1/ /media/david/USB/

note: run on mac
cd skraak.kiwi
using Franklin
serve()

=#
# New one, without noise and distance, does not do :box anymore therefore requires new db schema, (rolled back for now)
function aggregate_labels(
    actual::String = "actual.csv",
    outfile::String = "labels.csv",
    hdr::Bool = false, #header for outfile
)::DataFrame
    df = DataFrame(CSV.File(actual))

    # location, f, start_time, end_time
    @transform!(df, @byrow :location = split(split(:file, "/")[2], "-")[1])
    @transform!(df, @byrow :f = split(split(:file, "/")[2], "-")[5] * ".WAV")
    #= Schema change
    @transform!(df, @byrow :start_time = split(split(:file, "/")[2], "-")[end-1])
    @transform!(
        df,
        @byrow :end_time = chop(split(split(:file, "/")[2], "-")[end], tail = 4)
    )=#
    @transform!(
        df,
        @byrow :box = "[$(split(split(:file, "/")[2], "-")[end-1]), $(chop(split(split(:file, "/")[2], "-")[end], tail=4))]"
    )

    # male, female, duet, not
    @transform!(df, @byrow @passmissing :male = split(:file, "/")[1] == "M" ? true : false)
    @transform!(
        df,
        @byrow @passmissing :female = split(:file, "/")[1] == "F" ? true : false
    )
    @transform!(df, @byrow @passmissing :duet = split(:file, "/")[1] == "D" ? true : false)
    @transform!(
        df,
        @byrow @passmissing :not_kiwi =
            split(:file, "/")[1] in ["KA", "KE", "N", "Q"] ? true : false
    )

    # other_label
    @transform!(
        df,
        @byrow @passmissing :other_label =
            split(:file, "/")[1] in ["KA", "KE", "Q"] ? split(:file, "/")[1] : missing
    )

    # remove unwanted cols, rename f to file
    select!(df, Not([:file]))
    rename!(df, :f => :file)

    CSV.write(outfile, df; header = hdr)
    return df
end

"""
audiodata_db(df::DataFrame, table::String)

Use to upload labels to AudioData.duckdb

Takes a dataframe and inserts into AudioData.db table.

audiodata_db(df, "pomona_labels_20230418")

using DataFrames, DBInterface, DuckDB, Random
"""
function audiodata_db(df::DataFrame, table::String)
    if Sys.islinux()
        con = DBInterface.connect(DuckDB.DB, "/media/david/SSD1/AudioData.duckdb")
    else
        con = DBInterface.connect(DuckDB.DB, "/Volumes/SSD1/AudioData.duckdb")
    end
    temp_name = randstring(6)
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
