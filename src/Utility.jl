# Utility.jl

module Utility

"""
Utility submodules:
    file_metadata_to_df
	twilight_tuple_local_time
    UTCtoNZDT
"""

export file_metadata_to_df, twilight_tuple_local_time, UTCtoNZDT

using CSV, DataFrames, Dates, Glob, HTTP, JSON, TimeZones, WAV, XMLDict
using DelimitedFiles #???

"""
file_metadata_to_df()

This function takes a file name, extracts wav metadata, gpx location, recording period start/end and saves to a database table

It is in the same place the wav files are, usually a removeable drive 
but could be on the Linux beasts internal drive.

This function needs raw audiomoth wav files and a gpx.

used like:
folders = glob("*/*/")
for folder in folders
    cd(folder)
    df = file_metadata_to_df()
    audiodata_db(df, "pomona_files")
    cd("/Volumes/Pomona-2/")
end

using DataFrames, Dates, DelimitedFiles, DuckDB, Glob, JSON3, Random, TimeZones, WAV, XMLDict
"""

function file_metadata_to_df()
    #con = DBInterface.connect(DuckDB.DB, "/Users/davidcary/Desktop/AudioData.db")
    #stmt = DBInterface.prepare(con, "INSERT INTO pomona_files VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

    #Initialise dataframe with columns: disk, location, trip_date, file, lattitude, longitude, start_recording_period_localt, finish_recording_period_localt, duration, sample_rate, zdt, ldt, moth_id, gain, battery, temperature
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
        zdt = String[],
        ldt = String[],
        moth_id = String[],
        gain = String[],
        battery = Float64[],
        temperature = Float64[],
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

    #So I know what it is doing
    println(raw_path_vec)

    #Loop over file list
    for file in wav_list
        #print(file)
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
        zdt = Dates.format(preformatting_zdt, "yyyy-mm-dd HH:MM:SSzzzz")
        preformatting_ldt = astimezone(preformatting_zdt, tz"Pacific/Auckland")
        ldt = Dates.format(preformatting_ldt, "yyyy-mm-dd HH:MM:SSzzzz")

        moth_id = comment_vector[8]
        gain = comment_vector[10]
        #index back from end because if V > 4.9 the wording chaaanges
        battery = parse(Float64, chop(comment_vector[end-4], tail = 1))
        temperature = parse(Float64, chop(comment_vector[end], tail = 2))

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
            zdt,
            ldt,
            moth_id,
            gain,
            battery,
            temperature,
        ]
        push!(df, row)

        #DBInterface.execute(stmt, row)
        #. for each file processed
        print(".")
    end
    return df
    #DBInterface.close!(con)
end

"""
twilight_tuple_local_time(dt::Date)

Takes a date and returns a tuple with local time twilight times. Use to make a Dataframe then csv.
Queries api.sunrise-sunset.org

was using civil_twilight_end, civil_twilight_begin, changed to sunrise, sunset

Use like this:
Using CSV, Dates, RataFrames, Skraak
df = DataFrame(Date=[], Dawn=[], Dusk=[])
dr = Dates.Date(2019,01,01):Dates.Day(1):Dates.Date(2020,12,31)
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

"""
UTCtoNZDT(files::Vector{String})

Takes a list of moth files and rewrites UTC filenames to NZDT, because since
reconfiguring my moths at start of daylight saving they are recording UTC
filenames which is not consistent with the way my notebook works.

a = glob("*/2022-12-17/")
for folder in a
cd(folder)
println(folder)
files = glob("*.WAV")
UTCtoNZDT(files)
cd("/media/david/Pomona-2")
end

using Dates
"""
function UTCtoNZDT(files::Vector{String})
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

        dt = DateTime(ye, mo, da, ho, mi, se)
        # Note assumes daylight saving
        new_date = dt + Dates.Hour(13)
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

#=
using Images, Glob
a=glob("*/*.png")
for file in a
    resize_image!(file)
end
works really fast
=#
function resize_image!(name::String, x::Int64=224, y::Int64=224)
        small_image = imresize(load(name), (x, y))
        save(name, small_image)
end

end # module
