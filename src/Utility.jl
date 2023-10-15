# Utility.jl

using CSV, DataFrames, Dates, DBInterface, DSP, DuckDB, Glob, HTTP, Images, JSON, PNGFiles, Random, SHA, TimeZones, WAV, XMLDict 

export back_one_hour!,
    check_png_wav_both_present,
    file_metadata_to_df,
    secondary_dataset,
    resize_image!,
    twilight_tuple_local_time,
    utc_to_nzdt!

"""
back_one_hour!(files::Vector{String})

This function takes a vector of file paths and renames each file in the
vector by changing the name of the file to the name of the file created one
hour before the original file. The new name format is yyyymmdd_HHMMSS.tmp,
which represents the time stamp of the original file minus one hour. This
function avoids force=true with mv, since new file names may already exist
and mv will stacktrace leaving a big mess to tidy up.

Args:

•  files (Vector{String}): A vector of strings where each element is
a path to a file.

Returns: Nothing - This function only renames files and saves them.

I use this to turn the clock back at the end of daylight saving.
"""
function back_one_hour!(files::Vector{String})
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

        new_date = dt - Dates.Hour(1)
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
        p = glob("$folder/*.png")
        for png in p
            !isfile(chop(png, tail = 3) * "wav") && println(png)
        end
    end
end

#=
used like:
using Glob, Skraak, CSV
folders=glob("*/2023-09-11*/")
for folder in folders
cd(folder)
    try
        df = Skraak.file_metadata_to_df()
        CSV.write("/media/david/Pomona-3/Pomona-3/pomona_files.csv", df; append=true)
    catch
        @warn "error with $folder"
    end
cd("/media/david/Pomona-3/Pomona-3/")
end

Then using duckdb cli from SSD:
duckdb AudioData.duckdb
show tables;
SELECT * FROM pomona_files;
COPY pomona_files FROM '/media/david/Pomona-3/Pomona-3/pomona_files_20230722.csv';
SELECT * FROM pomona_files;

Then backup with:
EXPORT DATABASE 'AudioDataBackup_2023-07-29';
.quit
Then quit and backup using cp on the db file

Then rsync ssd to usb
rsync -avzr  --delete /media/david/SSD1/ /media/david/USB/
=#
"""
file_metadata_to_df()

This function takes a file name, extracts wav metadata, gpx location, recording period start/end and returnes a dataframe.

This function needs raw audiomoth wav files and a gpx.
This function needs /media/david/SSD1/dawn_dusk.csv

using DataFrames, Dates, DelimitedFiles, DuckDB, Glob, JSON3, Random, SHA, TimeZones, WAV, XMLDict
"""

function file_metadata_to_df()
    # Initialise dataframe with columns: disk, location, trip_date, file, lattitude, longitude, start_recording_period_localt, finish_recording_period_localt, duration, sample_rate, zdt, ldt, moth_id, gain, battery, temperature
    df = DataFrame(
        disk = String[],
        location = String[],
        trip_date = String[],
        file = String[],
        latitude = Float64[],
        longitude = Float64[],
        start_recording_period_localt = String[],
        finish_recording_period_localt = String[],
        duration = Float64[],
        sample_rate = Int[],
        utc = String[],
        ldt = String[],
        moth_id = String[],
        gain = String[],
        battery = Float64[],
        temperature = Float64[],
        sha2_256 = String[],
        night = Bool[],
    )

    #Get WAV list for folder
    wav_list = glob("*.WAV") |> sort

    #Return empty df if nothing in the folder
    if length(wav_list) == 0
        return df
    end

    #Get path info from file system
    raw_path_vec = split(pwd(), "/")[end-2:end]

    disk = raw_path_vec[1]
    location = raw_path_vec[2]
    trip_date = raw_path_vec[3]

    #Get location, assumes 1 gpx is in the follder
    waypoint = glob("*.gpx")
    length(waypoint) != 1 && @error "no gpx file in $trip_date $location"
    loc = read(waypoint[1], String) |> xml_dict

    latitude = parse(Float64, (loc["gpx"]["wpt"][:lat]))
    longitude = parse(Float64, (loc["gpx"]["wpt"][:lon]))

    #Start of recording period
    _, _, _, binary_metadata_start = wavread(wav_list[1])
    c_v_s = split(wav_info_read(binary_metadata_start)[:ICMT], " ")
    comment_vector_start = length(c_v_s) < 22 ? c_v_s : c_v_s[1:19]
    date_start = split(comment_vector_start[4], "/")
    time_start = split(comment_vector_start[3], ":")
    tz_start = chop(comment_vector_start[5], head = 4, tail = 1)
    time_zone_start = isempty(tz_start) ? "+00" : tz_start
    #zdt1 = ZonedDateTime(parse(Int, date_start[3]), parse(Int, date_start[2]), parse(Int, date_start[1]), parse(Int, time_start[1]), parse(Int, time_start[2]), parse(Int, time_start[3]), tz"UTC")
    time_string_start =
        date_start[3] *
        "-" *
        date_start[2] *
        "-" *
        date_start[1] *
        "T" *
        time_start[1] *
        ":" *
        time_start[2] *
        ":" *
        time_start[3] *
        "." *
        "000" *
        time_zone_start
    zdt1 = ZonedDateTime(time_string_start)
    start_recording_period_localt =
        Dates.format(astimezone(zdt1, tz"Pacific/Auckland"), "yyyy-mm-dd HH:MM:SSzzzz")

    #End of recording period
    _, _, _, binary_metadata_end = wavread(wav_list[end])
    c_v_e = split(wav_info_read(binary_metadata_end)[:ICMT], " ")
    comment_vector_end = length(c_v_e) < 22 ? c_v_e : c_v_e[1:19]
    date_end = split(comment_vector_end[4], "/")
    time_end = split(comment_vector_end[3], ":")
    tz_end = chop(comment_vector_start[5], head = 4, tail = 1)
    time_zone_end = isempty(tz_end) ? "+00" : tz_end
    #zdt2 = ZonedDateTime(parse(Int, date_end[3]), parse(Int, date_end[2]), parse(Int, date_end[1]),parse(Int, time_end[1]), parse(Int, time_end[2]), parse(Int, time_end[3]), tz"UTC")
    time_string_end =
        date_end[3] *
        "-" *
        date_end[2] *
        "-" *
        date_end[1] *
        "T" *
        time_end[1] *
        ":" *
        time_end[2] *
        ":" *
        time_end[3] *
        "." *
        "000" *
        time_zone_end
    zdt2 = ZonedDateTime(time_string_end)
    finish_recording_period_localt =
        Dates.format(astimezone(zdt2, tz"Pacific/Auckland"), "yyyy-mm-dd HH:MM:SSzzzz")

    dict = Skraak.construct_dawn_dusk_dict("/media/david/SSD1/dawn_dusk.csv")

    #So I know what it is doing
    println(raw_path_vec)

    #Loop over file list
    for file in wav_list
        #print(file)
        try
            audio_data, sample_rate, _, binary_metadata = wavread(file)
            c_v = split(wav_info_read(binary_metadata)[:ICMT], " ")
            comment_vector = length(c_v) < 22 ? c_v : c_v[1:19]

            duration = Float64(length(audio_data) / sample_rate)

            date = split(comment_vector[4], "/")
            time = split(comment_vector[3], ":")
            tz = chop(comment_vector[5], head = 4, tail = 1)
            time_zone = isempty(tz) ? "+00" : tz
            #preformatting_zdt = ZonedDateTime(parse(Int, date[3]), parse(Int, date[2]), parse(Int, date[1]), parse(Int, time[1]), parse(Int, time[2]), parse(Int, time[3]), tz"UTC")
            time_string =
                date[3] *
                "-" *
                date[2] *
                "-" *
                date[1] *
                "T" *
                time[1] *
                ":" *
                time[2] *
                ":" *
                time[3] *
                "." *
                "000" *
                time_zone
            preformatting_zdt = ZonedDateTime(time_string)
            #zdt = Dates.format(preformatting_zdt, "yyyy-mm-dd HH:MM:SSzzzz")
            preformatting_utc = astimezone(preformatting_zdt, tz"UTC")
            utc = Dates.format(preformatting_utc, "yyyy-mm-dd HH:MM:SSzzzz")
            preformatting_ldt = astimezone(preformatting_zdt, tz"Pacific/Auckland")
            ldt = Dates.format(preformatting_ldt, "yyyy-mm-dd HH:MM:SSzzzz")

            moth_id = comment_vector[8]
            gain = comment_vector[10]
            #index back from end because if V > 4.9 the wording chaaanges
            battery = parse(Float64, chop(comment_vector[end-4], tail = 1))
            temperature = parse(Float64, chop(comment_vector[end], tail = 2))

            sha2_256 = bytes2hex(sha256(file))

            #assumes 15 minute file and calculates on half way time

            nt = Skraak.night(DateTime(preformatting_ldt + Minute(7) + Second(30)), dict)

            #Populate row to push into df
            row = [
                disk,
                location,
                trip_date,
                file,
                latitude,
                longitude,
                start_recording_period_localt,
                finish_recording_period_localt,
                duration,
                Int(sample_rate),
                utc,
                ldt,
                moth_id,
                gain,
                battery,
                temperature,
                sha2_256,
                nt,
            ]
            push!(df, row)

            print(".")
        catch
            @warn "error with $folder $file"
        end
    end
    return df
end

#=
make dataset for image model
drive   location    trip_date   file    box     label

using CSV, DataFrames, DataFramesMeta, Glob

m = DataFrame(CSV.File("/media/david/USB/images_model/P_Male.csv"))

#run from media/david
function get_drive_and_trip_date(location, file)
    a=glob("Pomona-*/Pomona-*/$location/*/$file")
    length(a) > 0 ? b=split(a[1], "/") : b=missing
    return b
end

c = DataFrame(CSV.File("/media/david/USB/SecondaryModel_COF/close.csv"))
#note: dropmissing!(df) or @transform df @byrow @passmissing or delete rows that dont work
@transform!(c, @byrow :trip_date=get_drive_and_trip_date(:location, :file)[4])
@transform!(c, @byrow :drive=get_drive_and_trip_date(:location, :file)[1])
CSV.write("/media/david/USB/SecondaryModel_COF/close.csv", c)

#get trip date
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
;cd /media/david
CSV.write("/media/david/USB/Aggregate.csv", df)

df2=df[4421:4521, :]
=#

"""
secondary_dataset(df::DataFrame)

Takes a dataframe and makes png spectro images for secondary classifier.
I used this to make my original MFDN and COF, Noise datasets, from data that was origieally tagged in avianz, I think.
Should be run from /media/david

using DSP, WAV, DataFrames, CSV, Glob, Images, PNGFiles
"""
function secondary_dataset(df::DataFrame)
    for row in eachrow(df)
        signal, freq = wavread(
            "$(row.drive)/$(row.drive)/$(row.location)/$(row.trip_date)/$(row.file)",
        )
        row.box[1] * freq > 1 ? st = floor(Int, (row.box[1] * freq)) : st = 1
        row.box[2] * freq < length(signal) ? en = ceil(Int, (row.box[2] * freq)) :
        en = length(signal)
        sample = signal[Int(st):Int(en)]
        name = "$(row.location)-$(row.trip_date)-$(chop(row.file, tail=4))-$(Int(floor(row.box[1])))-$(Int(ceil(row.box[2])))"
        outfile = "/home/david/ImageSet/$(row.label)/$name"
        image = get_image_from_sample(sample, freq)
        PNGFiles.save("$outfile.png", image)
        print(".")
    end
    println("done")
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

