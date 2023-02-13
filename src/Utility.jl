# Utility.jl

module Utility

"""
Utility submodules:
	twilight_tuple_local_time
    UTCtoNZDT
"""

export twilight_tuple_local_time, UTCtoNZDT

using CSV, DataFrames, Dates, HTTP, JSON, TimeZones

"""
twilight_tuple_local_time(dt::Date)

Takes a date and returns a tuple with local time twilight times. Use to make a Dataframe then csv.
Queries api.sunrise-sunset.org

Use like this:

df = DataFrame(Date=[], Dawn=[], Dusk=[])
dr = Dates.Date(2023,01,01):Dates.Day(1):Dates.Date(2024,12,31)
for day in dr
    q = twilight_tuple_local_time(day)
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
    dusk_utc = get(resp3, "civil_twilight_end", "missing")
    dusk_utc_zoned = ZonedDateTime(dusk_utc, "yyyy-mm-ddTHH:MM:SSzzzz")
    dusk_local = astimezone(dusk_utc_zoned, tz"Pacific/Auckland")
    dusk_string = Dates.format(dusk_local, "yyyy-mm-ddTHH:MM:SS")
    dawn_utc = get(resp3, "civil_twilight_begin", "missing")
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

end # module
