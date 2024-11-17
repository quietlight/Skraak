# Utility.jl

export dawn_dusk_of_sunrise_sunset,
    get_sunrise_sunset_utc,
    make_spectro_from_file,
    move_one_hour!,
    resample_to_16000hz,
    resample_to_8000hz,
    resize_image!,
    utc_to_nzdt!,
    check_png_wav_both_present

using CSV,
    DataFrames,
    Dates,
    Glob,
    HTTP,
    ImageTransformations,
    JSON3,
    TimeZones,
    WAV,
    DataFramesMeta
#XMLDict, DBInterface, DSP, DuckDB, PNGFiles, Random, SHA

"""
check_png_wav_both_present(folders::Vector{String})

Given a vector of folder paths, this function checks whether each folder
contains a matching .png and .wav file. If a folder does not contain a
matching .wav file, a message is printed to indicate the folder path where
the .wav file is missing.

Args:

•  folders (Vector{String}): A vector of strings where each element
is a path to a directory.

Returns: Nothing - This function only prints messages to the console.
"""
function check_png_wav_both_present(folders::Vector{String})
    println("No matching wav: ")
    for folder in folders
        println(folder)
        println("Missing wav:")
        p = Glob.glob("$folder/*.png")
        for png in p
            !isfile(chop(png, tail = 3) * "wav") && println(png)
        end
        println("Missing png:")
        w = Glob.glob("$folder/*.[W,w][A,a][V,v]")
        for wav in w
            !isfile(chop(wav, tail = 3) * "png") && println(wav)
        end
    end
end

"""
resize_image!(name::String, x::Int64=224, y::Int64=224)

This function resizes an image with a specified name to a smaller size with
dimensions x and y. By default, the dimensions are set to 224 x 224, which
is common for image classification models.

Args:

•  name (String): A string representing the name and path of the
image file that needs to be resized.

•  x (Int64): An integer representing the desired width of the
resized image. Default is set to 224.

•  y (Int64): An integer representing the desired height of the
resized image. Default is set to 224.

Returns: Nothing - This function only resizes the image and saves it to the
same path.

Use it like this:
using Images, Glob
a=Glob.glob("*/*.png")
for file in a
resize_image!(file)
end

works really fast
"""
function resize_image!(name::String, x::Int64 = 224, y::Int64 = 224)
    small_image = ImageTransformations.imresize(load(name), (x, y))
    save(name, small_image)
end

#dawn_dusk is local time, sunrise_sunret is utc
function dawn_dusk_of_sunrise_sunset(file::String)
    df = DataFrames.DataFrame(CSV.File(file))
    @transform!(
        df,
        @byrow :Dawn = (
            ZonedDateTime((:sunrise |> String), "yyyy-mm-ddTHH:MM:SSzzzz") |>
            x ->
                astimezone(x, tz"Pacific/Auckland") |>
                x -> Dates.format(x, "yyyy-mm-ddTHH:MM:SS")
        )
    )
    @transform!(
        df,
        @byrow :Dusk =
            ZonedDateTime((:sunset |> String), "yyyy-mm-ddTHH:MM:SSzzzz") |>
            x ->
                astimezone(x, tz"Pacific/Auckland") |>
                x -> Dates.format(x, "yyyy-mm-ddTHH:MM:SS")
    )
    select!(df, :day => :Date, :Dawn, :Dusk)
    CSV.write("dawn_dusk.csv", df)
end

#use  this function to get a date range of data, saves to csv in cwd and  returns df  
function get_sunrise_sunset_utc(dr::StepRange{Date,Day})
    # C05 co-ordinates hard coded into function
    cols = [
        "day",
        "solar_noon",
        "sunrise",
        "day_length",
        "sunset",
        "civil_twilight_end",
        "astronomical_twilight_end",
        "astronomical_twilight_begin",
        "nautical_twilight_begin",
        "civil_twilight_begin",
        "nautical_twilight_end",
    ]
    df = DataFrames.DataFrame([name => [] for name in cols])
    for day in dr
        resp =
            HTTP.get(
                "https://api.sunrise-sunset.org/json?lat=-45.50608&lng=167.47822&date=$day&formatted=0",
            ) |>
            #x -> String(x.body) |> JSON.Parser.parse |> x -> get(x, "results", "missing")
            x -> String(x.body) |> x -> JSON3.read(x) |> x -> get(x, "results", "missing")
        data = copy(resp)
        data[:day] = string(day)
        push!(df, data)
        print("$day  ")
        sleep(3)
    end
    CSV.write("sunrise_sunset_utc.csv", df)
    return df
end

"""
move_one_hour!(files::Vector{String}, operator)

This function takes a vector of file paths and renames each file in the
vector by changing the name of the file to the name of the file created one
hour before the original file. The new name format is yyyymmdd_HHMMSS.tmp,
which represents the time stamp of the original file minus (or plus) one hour. This
function avoids force=true with mv, since new file names may already exist
and mv will stacktrace leaving a big mess to tidy up.

Args:

•  files (Vector{String}): A vector of strings where each element is
a path to a file.

Returns: Nothing - This function only renames files and saves them.

I use this to turn the clock back at the end of daylight saving.

Assumes WAV files
"""
function move_one_hour!(files::Vector{String}, operator)
    @assert operator == (+) || operator == (-)
    fix_extension_of_files = []
    for old_file in files
        # Extract the date and time of the original file using string chopping
        a = chop(old_file, tail = 4)
        d, t = split(a, "_")

        ye = parse(Int64, d[1:4])
        mo = parse(Int64, d[5:6])
        da = parse(Int64, d[7:8])
        ho = parse(Int64, t[1:2])
        mi = parse(Int64, t[3:4])
        se = parse(Int64, t[5:6])

        dt = DateTime(ye, mo, da, ho, mi, se)

        #new_date = dt - Dates.Hour(1)
        new_date = operator(dt, Dates.Hour(1))
        # Must drop the WAV extension to avoiding force=true 
        # with  mv, since  the new file name may already exist and mv
        # will stacktrace leaving a big mess to tidy up.
        base_file = Dates.format(new_date, "yyyymmdd_HHMMSS")
        temp_file = base_file * ".tmp"

        # Tuple to tidy extensions later
        tidy = (temp_file, base_file * ".WAV")

        mv(old_file, temp_file)
        push!(fix_extension_of_files, tidy)
        print(".")
    end
    for item in fix_extension_of_files
        mv(item[1], item[2])
    end
    print("Tidy\n")
end

"""
utc_to_nzdt!files::Vector{String})

Takes a list of moth files and rewrites UTC filenames to NZDT, because since
reconfiguring my moths at start of daylight saving they are recording UTC
filenames which is not consistent with the way my notebook works.

a = Glob.glob("*/2022-12-17/")
for folder in a
cd(folder)
println(folder)
files = Glob.glob("*.WAV")
utc_to_nzdt!files)
cd("/media/david/Pomona-2")
end

using Dates, TimeZones
"""
function utc_to_nzdt!(files::Vector{String})
    fix_extension_of_files = []
    for old_file in files
        a = chop(old_file, tail = 4)
        d, t = split(a, "_")

        ye = parse(Int64, d[1:4])
        mo = parse(Int64, d[5:6])
        da = parse(Int64, d[7:8])
        ho = parse(Int64, t[1:2])
        mi = parse(Int64, t[3:4])
        se = parse(Int64, t[5:6])

        dt = ZonedDateTime(ye, mo, da, ho, mi, se, tz"UTC")
        new_date = astimezone(dt, tz"Pacific/Auckland")
        # Must drop the WAV extension to avoiding force=true 
        # with  mv, since  the new file name may already exist and mv
        # will stacktrace leaving a big mess to tidy up.
        isfile(Dates.format(new_date, "yyyymmdd_HHMMSS") * ".tmp") ?
        base_file = Dates.format((new_date + Dates.Second(1)), "yyyymmdd_HHMMSS") :
        base_file = Dates.format(new_date, "yyyymmdd_HHMMSS")
        temp_file = base_file * ".tmp"

        # Tuple to tidy extensions later
        tidy = (temp_file, base_file * ".WAV")

        mv(old_file, temp_file)
        push!(fix_extension_of_files, tidy)
        print(".")
    end
    for item in fix_extension_of_files
        mv(item[1], item[2])
    end
    print("Tidy\n")
end

function resample_to_16000hz(signal, freq)
    signal = DSP.resample(signal, 16000.0f0 / freq; dims = 1)
    freq = 16000
    return signal, freq
end

function resample_to_8000hz(signal, freq)
    signal = DSP.resample(signal, 8000.0f0 / freq; dims = 1)
    freq = 8000
    return signal, freq
end

# Convert mp3's with: for file in *.mp3; do ffmpeg -i "${file}" -ar 16000 "${file%.*}.wav"; done
# Requires 16000hz wav's, works in current folder, need ffmpeg to convert mp3's to wavs at 16000hz
#= 
wavs = Glob.glob("*.wav")
for wav in wavs
    Skraak.make_spectro_from_file(wav)
end
=#
function make_spectro_from_file(file::String)
    signal, freq = WAV.wavread("$file")
    freq = freq |> Float32
    partitioned_signal = Iterators.partition(signal, 80000) #5s clips

    for (index, part) in enumerate(partitioned_signal)
        length(part) > 50000 && begin
            outfile = "$(chop(file, head=0, tail=4))__$(index)"
            image = Skraak.get_image_from_sample(part, freq)
            PNGFiles.save("$outfile.png", image)
        end
    end
end

# get the size in GB of a trip worth of data
list = glob("Pomona/*/2024-06-23/*")
#list=glob("*/2024-10-18/*")
function trip_filesize(list::String)
    x = []
    for item in list
        y = filesize(item)
        push!(x, y)
    end
    return sum(x) / 1000000000
end

#= Dont use, was the start of function below, useful as explanation
a=glob("Pomona/*/2022-10-08/")
for folder in a
    f=replace(folder, "2022-10-08/" => "2022-10-08")
    d=replace(folder, "2022-10-08/" => "")
    println("rsync -avzr $f /media/david/Pomona-1/$d")
    run(`rsync -avzr $f /media/david/Pomona-1/$d`)
end
=#

#=for copying over a trip worth of data from 1 Pomona drive to another
a1=glob("Pomona/*/2023-12-25/")
a2=glob("Pomona/*/2024-05-05/")
a3=glob("Pomona/*/2024-06-23/")
d=[a1;a2;a3]

copy_over(d, "Pomona-3")
=#
function copy_over(list::Vector{String}, dst::String)
    for folder in list
        folder_name::String = split(folder, "/")[end-1]
        f = replace(folder, "$folder_name/" => "$folder_name")
        d = replace(folder, "$folder_name/" => "")
        println("rsync -avzr $f /media/david/$dst/$d")
        run(`rsync -avzr $f /media/david/$dst/$d`)
    end
end

# delete originating files
##### BE CAREFUL
#for folder in d
# rm(folder, recursive=true)
#end
