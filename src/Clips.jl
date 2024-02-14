# Clips.jl

export make_clips, move_clips_to_folders

using CSV, DataFrames, Dates, DSP, Glob, JSON, PerceptualColourMaps, Random, TimeZones, WAV, PNGFiles, Images
using DataFramesMeta #: @transform!, @subset!, @byrow, @passmissing

"""
make_clips(preds_path::String, dawn_dusk_dict::Dict{Dates.Date, Tuple{Dates.DateTime, Dates.DateTime}} = construct_dawn_dusk_dict("/media/david/SSD1/dawn_dusk.csv"))

This function takes a preds.csv files and generates
file names, wav's, spectrograms etc to be reviewed.
it calls night() and may call construct_dawn_dusk_dict() unless the dict is globally defined and passed in

It should be run from Pomona-1/, Pomona-2/ or Pomona-3/, assumes it is, it uses the path
It saves  wav and png files to current working directory, ie Pomona-3
need to use a try/catch because the 2 assert functions thow an error to short circuit the function

using Glob, Skraak
predictions = glob("*/2023-09-11*/preds*")
predictions = glob("path/to/preds*")
for file in predictions #[1:6][7:12][13:18][19:24]
try
make_clips(file)
catch x
println(x)
end
end

if needed to change headers in preds csv
shift, control, f in subl
file,start_time,end_time,label
/media/david/Pomona-2,<project filters>, preds-2023-02-27.csv
file,start_time,end_time,absent,present

using Glob, CSV, DataFrames, DataFramesMeta, Dates, DSP, Plots, Random, WAV
"""

# Assumes run on linux
# Assumes function run from Pomona-1 or Pomona-2
#dawn_dusk_dict::Dict{Dates.Date,Tuple{Dates.DateTime,Dates.DateTime}} = construct_dawn_dusk_dict("/media/david/SSD1/dawn_dusk.csv",),
function make_clips(
    preds_path::String,
    label::Int = 1,
    night::Bool = true,
    dawn_dusk_dict = dddict,
)
    # Assumes function run from Pomona-1 or Pomona-2
    location, trip_date, _ = split(preds_path, "/")

    # Load and group data frame by file
    gdf =
        #! format: off
        DataFrame(CSV.File(preds_path)) |>
        x -> assert_not_empty(x, preds_path) |>
        x -> rename_column!(x, "1.0", "label") |> #can remove now, needs to be label
        x -> assert_detections_present(x, label, location, trip_date) |>
        x -> filter_positives!(x, label) |>
        insert_datetime_column! |>
        x -> night_or_day!(x, dawn_dusk_dict, night) |> #true=night, false=day
        group_by_file!
        #! format: on
    # Make clip and spectrogram
    for (k, v) in pairs(gdf)
        #file_name = chop(v.file[1], head = 2, tail = 4)
        file_name = path_to_file_string(v.file[1])
        start_times = v[!, :start_time] |> sort

        detections = cluster_detections(start_times)
        isempty(detections) && continue

        signal, freq = wavread("$location/$trip_date/$file_name.WAV")
        length_signal = length(signal)

        for detection in detections
            st, en = calculate_clip_start_end(detection, freq, length_signal)
            name = "$location-$trip_date-$file_name-$(Int(floor(st/freq)))-$(Int(ceil(en/freq)))"
            f = "Clips_$(today())"
            mkpath(f)
            outfile = "$f/$name"

            sample = signal[Int(st):Int(en)]
            wavwrite(sample, "$outfile.wav", Fs = Int(freq))

            #plot = plot_spectrogram(sample, freq)
            #savefig(plot, "$outfile.png")
            image = get_image_from_sample(sample, freq)
            PNGFiles.save("$outfile.png", image)
        end
        print(".")
    end
    println("\ndone $location/$trip_date \n")
end

#######################################################################

function assert_not_empty(df::DataFrame, preds_path::String)::DataFrame
    size(df) != (0, 0) ? (return df) : @error "Empty dataframe at $preds_path"
    #return df
end

function rename_column!(df::DataFrame, old_name::String, new_name::String)::DataFrame
    old_name in names(df) && rename!(df, old_name => new_name)
    return df
end

# assumes kiwi, binary classifier from opensoundscape
# needed to remove ::String annotation for location, trip_date to make it work
function assert_detections_present(
    df::DataFrame,
    label::Int,
    location,
    trip_date,
)::DataFrame
    label in levels(df.label) ? (return df) :
    @error "No detections for label = $label at $location/$trip_date"
end

# assumes kiwi
function filter_positives!(df::DataFrame, label)::DataFrame
    #filter!(row -> row.kiwi > 0, df)
    filter!(row -> row.label == label, df)
    return df
end

function path_to_file_string(path) #becareful path::String won't work: no method matching path_to_file_string(::InlineStrings.String31) line 70
    f = split(path, "/")[end] |> x -> split(x, ".") |> first
    #f = chop(file, head = 2, tail = 4)
    return f
end

function filename_to_datetime!(file)::DateTime
    #file_string = chop(file, head = 2, tail = 4)
    file_string = path_to_file_string(file)
    date_time =
        length(file_string) > 13 ? DateTime(file_string, dateformat"yyyymmdd_HHMMSS") :
        DateTime(
            (file_string[1:4] * "20" * file_string[5:end]),
            dateformat"ddmmyyyy_HHMMSS",
        )
    return date_time
end

function insert_datetime_column!(df::DataFrame)::DataFrame
    @transform!(df, @byrow :DateTime = filename_to_datetime!(String(:file)))
    return df
end

# calls night(), needs dawn_dusk_dict in local time format
function night_or_day!(
    df::DataFrame,
    dawn_dusk_dict::Dict{Dates.Date,Tuple{Dates.DateTime,Dates.DateTime}},
    night_time::Bool = true,
)::DataFrame
    night_time ? @subset!(df, @byrow night(:DateTime, dawn_dusk_dict)) :
    @subset!(df, @byrow !night(:DateTime, dawn_dusk_dict))
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

# assumes it is operating on 5 second clips
function calculate_clip_start_end(
    detection::Vector{Float64},
    freq::Float32,
    length_signal::Int64,
)::Tuple{Float64,Float64}
    first(detection) > 0 ? st = first(detection) * freq : st = 1
    (last(detection) + 5.0) * freq <= length_signal ? en = (last(detection) + 5.0) * freq :
    en = length_signal
    return st, en
end


# f neeeds to be an Int
function get_image_from_sample(sample, f) #sample::Vector{Float64}
    S = DSP.spectrogram(sample, 400, 2; fs = convert(Int, f))
    i = S.power
    if minimum(i) == 0.0
        l = i |> vec |> unique |> sort
        replace!(i, 0.0 => l[2])
    end
    image =
        #! format: off
        DSP.pow2db.(i) |>
        x -> x .+ abs(minimum(x)) |>
        x -> x ./ maximum(x) |>
        x -> reverse(x, dims = 1) |>
        x -> applycolourmap(x, cmap("L4")) |>
        x -> RGB.(x) |> 
        x -> imresize(x, 224, 224)
        #! format: on
    return image
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

#INBETWEEN STEP: use secondary model to sort clips, move clips into D, F, M, N, and hand classify, generate actual.csv.

"""
move_clips_to_folders(df::DataFrame)

Takes a 2 column dataframe: file, label
file must be list of png images, assumes wav's are there too
will move mp4's from video folder if they are present
"""
function move_clips_to_folders(df::DataFrame)
    p = glob("*.png")
    w = glob("*.[W,w][A,a][V,v]")
    @assert (first(df.file) |> x -> split(x, ".")[end] |> x -> x == "png") "df.file must be a list of png's"
    @assert issetequal(df.file, p) "All png files in dataframe must be present in folder"
    @assert issetequal(chop.(df.file, head = 0, tail = 4), chop.(w, head = 0, tail = 4)) "There must be a wav for every png in the dataframe"
    for row in eachrow(df)
        src = row.file
        dst = "$(row.label)/$(row.file)"
        mkpath("$(row.label)/")
        try
            mv(src, dst)
            mv(chop(src, tail = 3) * "wav", chop(dst, tail = 3) * "wav")
            if isdir(video)
                mkpath("video/$(row.label)/")
                mv(
                    "video/" * chop(src, tail = 3) * "mp4",
                    "video/" * chop(dst, tail = 3) * "mp4",
                )
            end
        catch e
            @info e
        end
    end
end

#=
For making colour images, not wired up into skraak yet.
Using for 24/7 and 250kHZ data.


using DSP, GLMakie, PNGFiles
function get_colour_image_from_sample(sample, f)
    dims = 224 #px
    S = DSP.spectrogram(sample[:, 1], 400, 2; fs = f)
    f = GLMakie.Figure(resolution = (dims, dims), figure_padding = 0)
    ax = GLMakie.Axis(f[1, 1], spinewidth = 0)
    GLMakie.hidedecorations!(ax)
    GLMakie.heatmap!(ax, (DSP.pow2db.(S.power))', colormap = :inferno)

    @assert size(f) == (dims, dims) "Wrong size"
    return f
end

function save_colour_image(f, outfile)
    try
        PNGFiles.save("$outfile.png", f)
    catch err
        @info "Saving $outfile.png failed\n$err"
    end
end
=#