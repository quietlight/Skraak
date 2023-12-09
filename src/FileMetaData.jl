# FileMetaData.jl

export file_metadata_to_df

using DataFrames, Dates, Glob, Random, SHA, TimeZones, WAV, XMLDict
#DelimitedFiles, DuckDB, JSON3

#=
used like:
using Glob, Skraak, CSV
folders=glob("*/2023-11-02/")
for folder in folders
cd(folder)
    try
        df = Skraak.file_metadata_to_df()
        CSV.write("/media/david/Pomona-3/Pomona-3/pomona_files_20231102.csv", df; append=true)
    catch
        @warn "error with $folder"
    end
cd("/media/david/Pomona-3/Pomona-3/")
end

Then using duckdb cli from SSD:
duckdb AudioData.duckdb
show tables;
SELECT * FROM pomona_files;
COPY pomona_files FROM '/media/david/Pomona-3/Pomona-3/pomona_files_20231019.csv';
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
