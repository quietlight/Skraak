module Skraak

export  make_clips, aggregate_labels, audiodata_db

include("Utility.jl")

using CSV, DataFrames, DataFramesMeta, Dates, DSP, Glob, HTTP, JSON, Plots, Random, TimeZones, WAV

"""
make_clips(preds_path::String, dawn_dusk_dict::Dict{Dates.Date, Tuple{Dates.DateTime, Dates.DateTime}} = construct_dawn_dusk_dict("/media/david/SSD1/dawn_dusk.csv"))

This function takes a preds.csv files and generates
file names, wav's, spectrograms etc to be reviewed.
it calls night() and may call construct_dawn_dusk_dict() unless the dict is globally defined and passed in

It should be run from Pomona-1/, Pomona-2/ or Pomona-3/, assumes it is, it uses the path
It saves  wav and png files to /home/david/Upload/
need to use a sry/catch because the 2 assert functions thow an error to short circuit the function

using Glob, Skraak
predictions = glob("path/to/preds*")
for file in predictions
    try
        make_clips(file)
    catch x
        println(x)
    end
end

if needed to change headers in preds csv
shift, control, f in subl
file,start_time,end_time,0.0,1.0
/media/david/Pomona-2,<project filters>, preds-2023-02-27.csv
file,start_time,end_time,absent,present

using Glob, CSV, DataFrames, DataFramesMeta, Dates, DSP, Plots, Random, WAV
"""
function make_clips(preds_path::String, dawn_dusk_dict::Dict{Dates.Date, Tuple{Dates.DateTime, Dates.DateTime}} = construct_dawn_dusk_dict("/media/david/SSD1/dawn_dusk.csv"))
    # Assumes function run from Pomona-1 or Pomona-2
    location, trip_date, _ = split(preds_path, "/")

    # Load and group data frame by file
    gdf = DataFrame(CSV.File(preds_path)) |>
            x -> assert_not_empty(x, preds_path) |>
            x -> rename_column!(x, "1.0", "kiwi") |>
            x -> assert_detections_present(x, location, trip_date) |> # assumes kiwi binary classifier
            filter_positives! |> # assumes kiwi
            insert_datetime_column! |>
            x -> exclude_daytime!(x, dawn_dusk_dict) |>
            group_by_file!

    # Make clip and spectrogram
    for (k, v) in pairs(gdf)
        file_name = chop(v.file[1], head = 2, tail = 4)
        start_times = v[!, :start_time] |> sort

        detections=cluster_detections(start_times)
        isempty(detections) && continue

        signal, freq = wavread("$location/$trip_date/$file_name.WAV")
        length_signal = length(signal)

        for detection in detections
            st, en = calculate_clip_start_end(detection, freq, length_signal)
            name = "$location-$trip_date-$file_name-$(Int(floor(st/freq)))-$(Int(ceil(en/freq)))"
            outfile = "/home/david/Upload/$name"

            sample = signal[Int(st):Int(en)]
            wavwrite(sample, "$outfile.wav", Fs = Int(freq))

            plot = plot_spectrogram(sample, freq);
            savefig(plot, "$outfile.png")
        end
        print(".")
    end
    println("\ndone $location/$trip_date \n")
end

#######################################################################

function assert_not_empty(df::DataFrame, preds_path::String)::DataFrame
    size(df) != (0,0) ? (return df) : @error "Empty dataframe at $preds_path"
    #return df
end

function rename_column!(df::DataFrame, old_name::String, new_name::String)::DataFrame
    old_name in names(df) && rename!(df, old_name => new_name)
    return df
end

# assumes kiwi, binary classifier from opensoundscape
function assert_detections_present(df::DataFrame, location::String, trip_date::String)::DataFrame
    1.0 in levels(df.kiwi) ? (return df) : @error "No kiwi detections at $location/$trip_date"
end

# assumes kiwi
function filter_positives!(df::DataFrame)::DataFrame
    filter!(row -> row.kiwi > 0, df)
end

function filename_to_datetime!(file)::DateTime
    file_string = chop(file, head = 2, tail = 4)
    date_time = length(file_string) > 13 ? DateTime(file_string, dateformat"yyyymmdd_HHMMSS") : DateTime((file_string[1:4] * "20" * file_string[5:end]), dateformat"ddmmyyyy_HHMMSS")
    return date_time
end

function insert_datetime_column!(df::DataFrame)::DataFrame
    @transform!(df, @byrow :DateTime = filename_to_datetime!(String(:file)))
    return df
end

# calls night(), needs dawn_dusk_dict in local time format
function exclude_daytime!(df::DataFrame, dawn_dusk_dict::Dict{Dates.Date, Tuple{Dates.DateTime, Dates.DateTime}})::DataFrame
    @subset!(df, @byrow night(:DateTime, dawn_dusk_dict))
    return df
end

function group_by_file!(df::DataFrame)
    gdf = groupby(df, :file)
    return gdf
end

function cluster_detections(start_times::Vector{Float64})::Vector{Vector{Float64}}
    s = Vector{Float64}[]
    t = Float64[start_times[1]]
    for time in start_times[2:end]
        if time - last(t) <= 15.0
            push!(t, time)
        else
            push!(s, copy(t))
            t = Float64[time]
        end
    end
    push!(s, copy(t))
    detections = filter(x -> length(x) > 1, s)
    return detections
end

function calculate_clip_start_end(detection::Vector{Float64}, freq::Float32, length_signal::Int64)::Tuple{Float64, Float64}
    first(detection) > 0 ? st = first(detection) * freq : st = 1
    (last(detection) + 5.0) * freq <= length_signal ?
        en = (last(detection) + 5.0) * freq : en = length_signal
    return st, en
end

function plot_spectrogram(sample::Vector{Float64}, freq::Float32)::Plots.Plot{Plots.GRBackend}
    S = DSP.spectrogram(sample[:, 1], 400, 2; fs = convert(Int, freq))
    plot=Plots.heatmap(
        S.time,
        S.freq,
        pow2db.(S.power),
        size = (448, 448),
        showaxis = false,
        ticks = false,
        legend = false,
        thickness_scaling = 0,
    );
    return plot
end


"""
construct_dawn_dusk_dict(file::String)::Dict{Date,Tuple{DateTime,DateTime}}
    sun = DataFrame(CSV.File(file))

Takes dawn dusk.csv and returns a dict to be consumeed by night().
~/dawn_dusk.csv
At present it goes from the start of 2019 to the end of 2024
The csv contains local time sunrise and sunset
I use this to decide if a file with a local time encoded name was recorded at night

dict = construct_dawn_dusk_dict("/Volumes/SSD1/dawn_dusk.csv")
dict = Utility.construct_dawn_dusk_dict("/media/david/SSD1/dawn_dusk.csv")

using CSV, DataFrames
"""
function construct_dawn_dusk_dict(file::String)::Dict{Date,Tuple{DateTime,DateTime}}
    sun = DataFrame(CSV.File(file))
    x = Tuple(zip(sun.Dawn, sun.Dusk))
    y = Dict(zip(sun.Date, x))
    return y
end

"""
night(call_time::DateTime, dict::Dict{Date, Tuple{DateTime, DateTime}})::Bool

Returns true if time is at night, ie between civil twilights, dusk to dawn.
Consumes dict from construct_dawn_dusk_dict

time=DateTime("2021-11-02T21:14:35",dateformat"yyyy-mm-ddTHH:MM:SS")
Utility.night(time, dict)
"""
function night(call_time::DateTime, dict::Dict{Date,Tuple{DateTime,DateTime}})::Bool
    dawn = dict[Date(call_time)][1]
    dusk = dict[Date(call_time)][2]
    if call_time <= dawn || call_time >= dusk
        return true
    else
        return false
    end
end

#######################################################################

#INBETWEEN STEP: use secondary model to sort clips, move clips into D, F, M, N, and hand classify, classify into COF, Noise, geneerate csv's.


"""
aggregate_labels(actual="actual_mfdn.csv", cof="predicted_cof.csv", noise="predicted_noise.csv", outfile="pomona_labels.csv")

file
[D, F, M, N]/C05-2023-04-15-20230219_223000-380-470.png

file	label
D/C05-2023-04-15-20230219_223000-380-470.png	C
D/C05-2023-04-15-20230220_000000-670-715.png	O
D/C05-2023-04-15-20230221_050000-435-473.png	F

file	label
D/C05-2023-04-15-20230219_223000-380-470.png	L
D/C05-2023-04-15-20230220_000000-670-715.png	M
D/C05-2023-04-15-20230221_050000-435-473.png	H
D/C05-2023-04-15-20230221_053000-810-850.png	T

This function prepares the csv output from my hand classification and secondary models and ouputs a df, and csv for insertion into AudioData.duckdb using the duckdb cli or using DFto.audiodata_db()

assumes run from Clips_xxxx-xx-xx folder and that actual_mfdn.csv, predicted_cof.csv, predicted_noise.csv, and that
assumes file names if not specified
saves a csv and also returns a dataframe

df=aggregate_labels()

using CSV, DataFrames, DataFramesMeta
"""
function aggregate_labels(
    actual::String = "actual_mfdn.csv",
    cof::String = "predicted_cof.csv",
    noise::String = "predicted_noise.csv",
    outfile::String = "pomona_labels.csv",
)::DataFrame
    a = DataFrame(CSV.File(actual))
    c = DataFrame(CSV.File(cof))
    rename!(c, :label => :distance)
    n = DataFrame(CSV.File(noise))
    rename!(n, :label => :noise)

    # make unique true not needed now I have renamed label column, but will help later maybe, in case of duplicate label names.
    x = leftjoin(a, c, on = :file)
    df = leftjoin(x, n, on = :file, makeunique = true)

    # location, f, box
    @transform!(df, @byrow :location = split(split(:file, "/")[2], "-")[1])
    @transform!(df, @byrow :f = split(split(:file, "/")[2], "-")[5] * ".WAV")
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

    # distance
    @transform!(df, @byrow @passmissing :close_call = :distance == "C" ? true : false)
    @transform!(df, @byrow @passmissing :ok_call = :distance == "O" ? true : false)
    @transform!(df, @byrow @passmissing :far_call = :distance == "F" ? true : false)

    # noise
    @transform!(df, @byrow @passmissing :low_noise = :noise == "L" ? true : false)
    @transform!(df, @byrow @passmissing :medium_noise = :noise == "M" ? true : false)
    @transform!(df, @byrow @passmissing :high_noise = :noise == "H" ? true : false)
    @transform!(df, @byrow @passmissing :terrible_noise = :noise == "T" ? true : false)

    # remove unwanted cols, rename f to file
    select!(df, Not([:file, :distance, :noise]))
    rename!(df, :f => :file)

    CSV.write(outfile, df)
    return df
end

"""
audiodata_db(df::DataFrame, table::String)

Use to upload labels to AudioData.duckdb

Takes a dataframe and inserts into AudioData.db table.

audiodata_db(df, "pomona_labels_20230405")

using DataFrames, DBInterface, DuckDB, Random
"""
function audiodata_db(df::DataFrame, table::String)
    temp_name = randstring(6)
    con = DBInterface.connect(DuckDB.DB, "/media/david/SSD1/AudioData.duckdb")
    #con = DBInterface.connect(DuckDB.DB, "/Volumes/SSD1/AudioData.duckdb")
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

# Rebuild skraak.kiwi, watch out for rats unless there is neew up to date data there already

end # module
