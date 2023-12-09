# Utility.jl

export check_png_wav_both_present,
    resize_image!, twilight_tuple_local_time, move_one_hour!, utc_to_nzdt!

using CSV, DataFrames, Dates, Glob, HTTP, Images, JSON, TimeZones, WAV
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
        p = glob("$folder/*.png")
        for png in p
            !isfile(chop(png, tail = 3) * "wav") && println(png)
        end
        println("Missing png:")
        w = glob("$folder/*.[W,w][A,a][V,v]")
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
a=glob("*/*.png")
for file in a
resize_image!(file)
end

works really fast
"""
function resize_image!(name::String, x::Int64 = 224, y::Int64 = 224)
    small_image = imresize(load(name), (x, y))
    save(name, small_image)
end

"""
twilight_tuple_local_time(dt::Date)

Takes a date and returns a tuple with local time twilight times. Use to make a Dataframe then csv.
Queries api.sunrise-sunset.org

was using civil_twilight_end, civil_twilight_begin, changed to sunrise, sunset

Use like this:
Using CSV, Dates, DataFrames, Skraak
df = DataFrame(Date=[], Dawn=[], Dusk=[])
dr = Dates.Date(2019,01,01):Dates.Day(1):Dates.Date(2024,12,31)
for day in dr
    q = Utility.twilight_tuple_local_time(day)
    isempty(q) ? println("fail $day") : push!(df, q)
    sleep(5)
end
CSV.write("dawn_dusk.csv", df)

using CSV, DataFrames, Dates, HTTP, JSON, TimeZones
"""
function twilight_tuple_local_time(dt::Date)
    # C05 co-ordinates hard coded into function
    resp1 = HTTP.get(
        "https://api.sunrise-sunset.org/json?lat=-45.50608&lng=167.47822&date=$dt&formatted=0",
    )
    resp2 = String(resp1.body) |> JSON.Parser.parse
    resp3 = get(resp2, "results", "missing")
    dusk_utc = get(resp3, "sunset", "missing")
    dusk_utc_zoned = ZonedDateTime(dusk_utc, "yyyy-mm-ddTHH:MM:SSzzzz")
    dusk_local = astimezone(dusk_utc_zoned, tz"Pacific/Auckland")
    dusk_string = Dates.format(dusk_local, "yyyy-mm-ddTHH:MM:SS")
    dawn_utc = get(resp3, "sunrise", "missing")
    dawn_utc_zoned = ZonedDateTime(dawn_utc, "yyyy-mm-ddTHH:MM:SSzzzz")
    dawn_local = astimezone(dawn_utc_zoned, tz"Pacific/Auckland")
    dawn_string = Dates.format(dawn_local, "yyyy-mm-ddTHH:MM:SS")
    date = Dates.format(dt, "yyyy-mm-dd")
    return (date, dawn_string, dusk_string)
end

#use  this function to get a date range of data, saves to csv in cwd and  returns df  
function twilight_tuple_local_time(dr::StepRange{Date,Day})
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
    df = DataFrame([name => [] for name in cols])
    for day in dr
        resp =
            HTTP.get(
                "https://api.sunrise-sunset.org/json?lat=-45.50608&lng=167.47822&date=$day&formatted=0",
            ) |>
            x -> String(x.body) |> JSON.Parser.parse |> x -> get(x, "results", "missing")
        resp["day"] = string(day)
        push!(df, resp)
        print("$day  ")
        sleep(3)
    end
    CSV.write("sunrise_sunset.csv", df)
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

a = glob("*/2022-12-17/")
for folder in a
cd(folder)
println(folder)
files = glob("*.WAV")
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
