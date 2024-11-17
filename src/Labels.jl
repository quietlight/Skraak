# Labels.jl

export actual_from_folders,
    aggregate_labels,
    audiodata_db,
    avianz_file_of_dict,
    avianz_of_raven, #not working right, 1 file per label I think
    check_change_avianz_species!,
    df_of_avianz_dict,
    dict_of_avianz_file,
    label_summary,
    one_hot_labels,
    prepare_df_for_raven,
    raven_of_avianz

using CSV, DataFrames, Glob, Random, DBInterface, DuckDB, JSON3, Dates
using DataFramesMeta: @transform!, @subset!, @byrow, @passmissing

#=
actual.csv must be list of qualified png file names: 
D/C05-2023-04-15-20230219_223000-380-470.png

using Glob, DataFrames, CSV
a=Glob.glob("[M,F,D,N]/*.png")
df = DataFrames.DataFrame(file=a)
CSV.write("actual_mfdn.csv", df)

make a folder D,F,M,N
mkpath.(["D", "F", "M", "N"])

move wavs to match pngs
df=DataFrames.DataFrame(CSV.File("actual_mfdn.csv"))
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
        paths = append!(paths, Glob.glob("$l/*.png"))
    end
    df = DataFrames.DataFrame(file = paths)
    CSV.write("actual.csv", df)
    return df
end

#=
df=aggregate_labels()

audiodata_db(df, "pomona_labels_20230418") NOT_WORKING maybe titles
to use cli, need to remove header row

duckdb /media/david/SSD1/AudioData.duckdb
COPY pomona_labels_20230418 FROM 'DB_Labels/pomona_labels_2023-12-28.csv';
COPY pomona_files FROM 'DB_Files/pomona_files_20231228.csv';

Then backup with:
EXPORT DATABASE 'AudioDataBackup_2023-11-14';
.quit
Then quit and backup using cp on the db file, dated copy

Then rsync ssd to usb
rsync -avzr  --delete /media/david/SSD1/ /media/david/USB/

note: run on mac
cd skraak.kiwi
julia-1.9
using Franklin
serve()

=#
# New one, without noise and distance, does not do :box anymore therefore requires new db schema
"""
aggregate_labels(actual="actual.csv", outfile="labels.csv")

file
[D, F, M, N]/C05-2023-04-15-20230219_223000-380-470.png

This function takes the csv output from my hand classification and ouputs a df, and csv for insertion into AudioData.duckdb using the duckdb cli or using audiodata_db()

assumes run from Clips_xxxx-xx-xx folder and that "actual.csv" present if not specified.
returns a dataframe

using CSV, DataFrames, DataFramesMeta
"""
function aggregate_labels(
    actual::String = "actual.csv",
    outfile::String = "labels.csv",
    hdr::Bool = false, #header for outfile 
)::DataFrame
    df = DataFrames.DataFrame(CSV.File(actual))

    # location, f, start_time, end_time
    @transform!(df, @byrow :location = split(split(:file, "/")[2], "-")[1])
    @transform!(df, @byrow :f = split(split(:file, "/")[2], "-")[5] * ".WAV")
    @transform!(df, @byrow :start_time = split(split(:file, "/")[2], "-")[end-1])
    @transform!(
        df,
        @byrow :end_time = chop(split(split(:file, "/")[2], "-")[end], tail = 4)
    )
    #@transform!( df, @byrow :box = "[$(split(split(:file, "/")[2], "-")[end-1]), $(chop(split(split(:file, "/")[2], "-")[end], tail=4))]")

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

function avianz_file_of_dict(f, payload)
    isfile(f) && cp(f, "$f.backup")
    open(f, "w") do io
        JSON3.write(io, payload)
    end
end

# ASSUMES 60 second files, also wav and txt file in same folder
# Raven selections.txt to AviaNZ .data 
# using CSV, DataFrames, JSON3
# uncomment line 'isfile(avianzf) && rm(f)' to remove .selection.txt file
# a=Glob.glob("*/*/*.Table.1.selections.txt")
# map(x -> avianz_of_raven(x), a)

function avianz_of_raven(f::String) #not working right, check
    df = DataFrames.DataFrame(CSV.File(f))
    data = Any[Dict([("Operator", "D"), ("Reviewer", "D"), ("Duration", 60.0)])]
    labels = Any[]
    for row in eachrow(df)
        label = [
            row."Begin Time (s)",
            row."End Time (s)",
            floor(Int, row."Low Freq (Hz)"),
            ceil(Int, row."High Freq (Hz)"),
            [Dict([("filter", "M"), ("species", row.Species), ("certainty", 100)])],
        ]
        push!(labels, label)
    end
    append!(data, labels)
    basef = f = replace(f, "Table.1.selections.txt" => "")
    isfile((basef * ".WAV")) ? avianzf = basef * "WAV.data" : avianzf = basef * "wav.data"
    avianz_file_of_dict(avianzf, data)
end

function dict_of_avianz_file(f)
    json_string = read(f, String)
    x = JSON3.read(json_string)
    return x
end

function df_of_avianz_dict(data)
    df = DataFrames.DataFrame(
        duration = Float64[],
        start_time = Float64[],
        end_time = Float64[],
        low_f = Float64[],
        high_f = Float64[],
        Species = String[],
    )
    dur = data[1]["Duration"]
    for (index1, item1) in enumerate(data[2:end])
        st, en, lf, hf = item1[1:4]
        sp = map(x -> x[:species], item1[5])
        lsp = length(sp)
        for (index2, item2) in enumerate(sp)
            push!(
                df,
                (
                    duration = dur,
                    start_time = st,
                    end_time = en,
                    low_f = lf,
                    high_f = hf,
                    Species = sp[index2],
                ),
            )
        end
    end
    return df
end

function prepare_df_for_raven(data)
    l = length(data.start_time)
    df = DataFrames.DataFrame(
        "Selection" => collect(1:l),
        "View" => ["Spectrogram 1" for x = 1:l],
        "Channel" => [1 for x = 1:l],
        "Begin Time (s)" => data.start_time,
        "End Time (s)" => data.end_time,
        "Low Freq (Hz)" => data.low_f,
        "High Freq (Hz)" => data.high_f,
        "Species" => data.Species,
    )
    return df
end

# using CSV, DataFrames, JSON3
function raven_of_avianz(file::String)
    data = dict_of_avianz_file(file)
    if length(data) >= 2 #ignores empty .data files
        df = df_of_avianz_dict(data) |> prepare_df_for_raven
        outfile = replace(
            file,
            ".WAV.data" => ".Table.1.selections.txt",
            ".wav.data" => ".Table.1.selections.txt",
        )
        CSV.write(outfile, df, delim = '\t')
    end
end

# Check then mutate all AviaNZ .data labels (Species column)
# using JSON3
# a=Glob.glob("*.WAV.data")
# check_and_change_labels!(a, "somethning", "something")

function check_change_avianz_species!(list::Vector{String}, wrong::String, right::String)
    function mutate_avianz_dict!(dct, wrong::String, right::String)
        didmutate = false
        data = copy(dct)
        for (index1, item1) in enumerate(data[2:end])
            for (index2, item2) in enumerate(item1[5])
                if item2[:species] == wrong
                    data[index1+1][5][index2][:species] = right
                    didmutate = true
                end
            end
        end
        return (didmutate, data)
    end
    for file in list
        x = dict_of_avianz_file(file)
        if length(x) >= 2 #ignores empty .data files
            didmutate, payload = mutate_avianz_dict!(x, wrong, right)
            if didmutate == true
                try
                    avianz_file_of_dict(file, payload)
                catch
                    @warn "Could not write file or backup: $file"
                end
            end
        end
    end
end

# if specified folder must include trailing /, can be "" for current folder
function label_summary(folder::String, avianz::Bool = true)
    if avianz == true
        files = Glob.glob("$(folder)*.['W','w']['A','a']['V','v'].data")
        df = DataFrames.DataFrame(
            duration = Float64[],
            start_time = Float64[],
            end_time = Float64[],
            low_f = Float64[],
            high_f = Float64[],
            Species = String[],
            File = String[],
        )
        for file in files
            df1 = dict_of_avianz_file(file) |> x -> df_of_avianz_dict(x)
            f = split(file, "/")[end] |> x -> replace(x, ".data" => "")
            df1.File = ["$f" for x = 1:length(df1.start_time)]
            df = vcat(df, df1)
        end
        select!(df, [:File, :duration, :start_time, :end_time, :low_f, :high_f, :Species])
        CSV.write("$(folder)label_summary_avianz-$(Dates.today()).csv", df, delim = '\t')
        return df
    else
        files = Glob.glob("$folder/*.Table.1.selections.txt")
        df = DataFrames.DataFrame(
            File = String[],
            start_time = Float64[],
            end_time = Float64[],
            low_f = Float64[],
            high_f = Float64[],
            Species = String[],
        )
        for file in files
            df1 = DataFrames.DataFrame(CSV.File(file))
            for d in eachrow(df1)
                f =
                    split(file, "/")[end] |>
                    x -> replace(x, ".Table.1.selections.txt" => "")
                push!(
                    df,
                    (
                        File = f,
                        start_time = d."Begin Time (s)",
                        end_time = d."End Time (s)",
                        low_f = d."Low Freq (Hz)",
                        high_f = d."High Freq (Hz)",
                        Species = d."Species",
                    ),
                )
            end
        end
        CSV.write("$(folder)label_summary_raven-$(Dates.today()).csv", df, delim = '\t')
        return df
    end
end

#allow 0.2 overlap each side
#round end_time down to nearest 5
function et(end_time::Float64)
    end_time % 5 > 0.2 ? (c0 = ceil(end_time / 5) * 5) : c0 = floor(end_time / 5) * 5
    return c0
end
#allow 0.2 overlap each side
#round start_time down to nearest 5
function st(start_time::Float64)
    start_time % 5 < 4.8 ? (f = floor(start_time / 5) * 5) : f = ceil(start_time / 5) * 5
    return f
end

#labels must be a df loaded from label_summary run over avianz data (not raven)
function one_hot_labels(labels::DataFrame)
    gdf = groupby(labels, :File)
    vdf = []
    for group in gdf
        dur = first(group.duration)
        nrows = dur ÷ 5
        seil = nrows * 5
        df = DataFrame(
            file = [first(group.File) for x = 1:nrows],
            start_time = collect(0:5:seil-1),
            end_time = collect(5:5:seil),
        )
        for row in eachrow(group)
            fst = st(row.start_time)
            @assert fst >= 0
            lst0 = et(row.end_time)
            #end time must not be greater than duration
            lst0 > dur ? lst = lst0 - 5 : lst = lst0
            @assert lst <= dur
            f_idx = fst ÷ 5 + 1 |> Int
            l_idx = lst ÷ 5 |> Int
            vect = [false for x = 1:nrows]
            for idx = f_idx:l_idx
                vect[idx] = true
            end
            col_name = row.Species
            df[!, Symbol(col_name)] = vect
        end
        #(names(df) |> length) > 4 && println(df)
        push!(vdf, df)
    end
    cdf1 = reduce(
        (x, y) ->
            outerjoin(x, y, matchmissing = :equal, on = intersect(names(x), names(y))),
        vdf,
    )
    cdf2 = coalesce.(cdf1, false)
end
