# CSVto.jl

module CSVto

"""
CSVto submodules:
    clip
    airtable_buckets
    dataset
    json


"""

export airtable, airtable_buckets, dataset, json, night, construct_dawn_dusk_dict

using Glob,
    CSV,
    DataFrames,
    DataFramesMeta,
    Dates,
    DelimitedFiles,
    DSP,
    HTTP,
    JSON,
    Plots,
    Random,
    TimeZones,
    WAV
"""
clip()

This function takes a preds.csv files and generates
file names, wav's, spectrograms etc to be uploaded to airtable for review.
It returns a dataframe to be piped into airtable_buckets()
it calls night() therefore night() must be available.

It should be run from Pomona-1/ or Pomona-2/, assumes it is, it uses the path
It saves  wav and png files to /home/david/Upload/
It returns a dataframe to be piped into airtable_buckets()
!!!now saves a csv instead

using Glob, Skraak
predictions = glob("path/to/preds*")
for file in predictions
CSVto.clip(file)
end

if needed to change headers in preds csv
shift, control, f in subl
file,start_time,end_time,0.0,1.0
/media/david/Pomona-2,<project filters>, preds-2023-02-27.csv
file,start_time,end_time,absent,present

using Glob, CSV, DataFrames, DataFramesMeta, Dates, DSP, Plots, Random, WAV
"""
function clip(file::String)
    # Assumes function run from Pomona-1 or Pomona-2
    location, trip_date, _ = split(file, "/")
    data = DataFrame(CSV.File(file))
    
    if !("file" in names(data))
        println("\nNo Detections at $location/$trip_date \n")
        return 
    elseif "1.0" in names(data)
        rename!(data, :"1.0" => :present)    
    end
    
    if length(data.file[1]) > 19
        @transform!(
            data,
            @byrow :DateTime =
                DateTime(chop(:file, head = 2, tail = 4), dateformat"yyyymmdd_HHMMSS")
                )
    # To handle DOC recorders
    else
        @transform!(
            data,
            @byrow :DateTime =
                DateTime((chop(:file, head = 2, tail = 4)[1:4] * "20" * chop(:file, head = 2, tail = 4)[5:end]), dateformat"ddmmyyyy_HHMMSS")
                )
    end

    gdf = groupby(data, :present)
    if (present = 1,) in keys(gdf)
        pres = gdf[(present = 1,)]
    else
        println("\nNo Detections at $location/$trip_date \n")
        return 
    end
    dawn_dusk_dict = construct_dawn_dusk_dict("/media/david/SSD1/dawn_dusk.csv")
    pres_night = @subset(
        pres,
        @byrow night(
            :DateTime,
            dawn_dusk_dict,
        )
    ) #throw away nights
    #sort!(pres_night)
    files = groupby(pres_night, :file)
    #airtable = DataFrame(a = Any[], b = Any[])
    airtable = DataFrame(
        FileName = String[],
        Image = String[],
        Audio = String[],
        StartTime = DateTime[],
        Length = Float64[],
        Location = String[],
        Trip = String[],
    )
    for (k, v) in pairs(files)
        file_start_time = v.DateTime[1]
        file_name = chop(v.file[1], head = 2, tail = 4)
        x = v[!, :start_time]
        sort!(x)
        s = []
        t = []
        for time in x
            if length(t) == 0
                push!(t, time)
            elseif time - last(t) <= 15.0
                push!(t, time)
            else
                push!(s, copy(t))
                deleteat!(t, 1:length(t))
                push!(t, time)
            end
        end
        push!(s, copy(t))
        deleteat!(t, 1:length(t))
        detections = filter(x -> length(x) > 1, s)
        #println(file_name, file_start_time, detections)
        if length(detections) > 0
            #load file
            signal, freq = wavread("$location/$trip_date/$file_name.WAV")
            for detection in detections
                #if the detection starts at start of the file I am cuttiing the first 0.1 seconds off.
                first(detection) > 0 ? st = first(detection) * freq : st = 1
                (last(detection) + 5.0) * freq <= length(signal) ?
                en = (last(detection) + 5.0) * freq : en = length(signal)
                sample = signal[Int(st):Int(en)]
                name = "$location-$trip_date-$file_name-$(Int(floor(st/freq)))-$(Int(ceil(en/freq)))"
                outfile = "/home/david/Upload/$name"
                #write a wav file
                wavwrite(sample, "$outfile.wav", Fs = Int(freq))
                #spectrogram
                n = 400
                fs = convert(Int, freq)
                S = spectrogram(sample[:, 1], n, n ÷ 200; fs = fs)
                heatmap(
                    S.time,
                    S.freq,
                    pow2db.(S.power),
                    size=(448,448),
                    showaxis=false,
                    ticks=false,
                    legend=false,
                    thickness_scaling=0,
                )
#=
                heatmap(
                    S.time,
                    S.freq,
                    pow2db.(S.power),
                    xguide = "Time [s]",
                    yguide = "Frequency [Hz]",
                )
=#
                savefig(outfile)
                #push to to airtable df
                push!(
                    airtable,
                    [
                        name,
                        "https://label-pomona.s3-ap-southeast-2.amazonaws.com/$name.png",
                        "https://label-pomona.s3-ap-southeast-2.amazonaws.com/$name.wav",
                        file_start_time,
                        (en - st) / freq,
                        location,
                        trip_date,
                    ],
                )
            end
        end
        print(".")
        #println(k, v)
    end
    # new bit to write a csv instead of return a dataframe to be bucketed into json for airtable. I can simplify the shape of the dataframe too sometime, all i need are the files for my new finder tagging workflow
    CSV.write("$location/$trip_date/segments-$location-$(string(today())).csv", airtable)
    println("\ndone $location/$trip_date \n")
    #return airtable (no longer needed as not using airtable anymore)
end

"""
airtable_buckets(dataframe)

Takes a dataframe with columns Audio, Trip, FileName, Image, Length, StartTime, Location, and returns json in ~/Airtable to be uploaded to airtable.
Intended to work with airtable() in a chain
"""
#=
This stuff throws an error when in docstring above:

#To divide json files into groups of 85 for upload to airtable
a=glob("*.json")
b=collect(Iterators.partition(a, 85))
for (index, value) in enumerate(b)
    mkpath("$index")
    for item in value
        mv(item, "$index/$item")
    end
    println("$index")
    println("$value\n\n")
end

See simplenote for upload script, remember to cd into the numbered folder
=#
function airtable_buckets(dataframe)
    e = floor(nrow(dataframe) / 10)
    f = round((nrow(dataframe) / 10 - e) * 10)
    g = []
    for i in 1:e
        for h in 1:10
            push!(g, i)
        end
    end
    for j in 1:f
        push!(g, e + 1)
    end
    dataframe.Bin = g
    grouped_dataframe = groupby(dataframe, :Bin)
    for group in grouped_dataframe
        index = randstring()
        io = open("/home/david/Airtable/$index.json", "w")
        write(io, """{"records":[""")
        for row in eachrow(group)
            write(io, """{"fields":{""")
            write(io, """"Audio":[{"url": "$(row.Audio)"}],""")
            write(io, """"Trip":"$(row.Trip)",""")
            write(io, """"File Name":"$(row."FileName")",""")
            write(io, """"Image":[{"url":"$(row.Image)"}],""")
            write(io, """"Length (seconds)":$(row.Length),""")
            write(io, """"Start Time": "$(row."StartTime")",""")
            write(io, """"Location": "$(row.Location)" """)
            write(io, """}}""")
            if row != last(group)
                write(io, """,""")
            end
        end
        write(io, "]}")
        close(io)
    end
end

"""
dataset()

This function takes a csv file in a folder and generates a dataset 
from a vector of labels. 

It is intended to be used for Male, Female, Close as it is correcting older annotations as it goes.

It works on CSV's from hand labelled AvianNZ style JSON.

It saves  wav files to /media/david/T7/TrainingData/2021-02-03_MFC/AudioData/  
and annotation files to a seperate directory as raven files. (it would be 
better for me to just build the csv direct, soon.)

NOTE: change the destination directory manually as required.

    # cd to the working directory, to run:
    julia
    using Glob, Skraak
    x = glob("*/2022-10-08")
    for i in x
    cd(i)
    #Skraak.CSVto.dataset(["Male", "Female", "Close"]) #For old label data
    #Skraak.CSVto.dataset(["Male", "Female", "Close", "Geese", "Kaka", "LTC", "Morepork", "Plover", "Not", "Kea"]) #For new label data
    Skraak.CSVto.dataset(["Kiwi", "Geese", "Kaka", "LTC", "Morepork", "Plover", "Not", "Kea"])

    #cd("/path/to/working/directory")
    cd("/media/david/Pomona-2/")
    end

using Glob, CSV, DataFrames, DelimitedFiles
"""

function dataset(labels::Vector{String})
    raw_data = glob("kiwi_data-*.csv")

    if length(raw_data) != 1
        message = pwd() * " has no kiwi_data csv present, or more than 1"
        return message

    else
        data_frame = DataFrame(CSV.File(raw_data[1]))
        #data_frame = filter(row -> row.species != missing, DataFrame(CSV.File(raw_data[1])))

        for row in eachrow(data_frame)
            #=
            # correct to Male, Female, Close to match newer annotations
            if row.species == "K-M"
                row.species = "Male"

            elseif row.species == "K-F"
                row.species = "Female"

            elseif row.species == "K-Close"
                row.species = "Close"

                # correct K-MF to Male plus another identical row with species=Female
            elseif row.species == "K-MF"
                row.species = "Male"
                push!(data_frame, merge(row, (species = "Female",)))
            end
            =#
            if row.species == "K-Set"
                row.species = "Kiwi"

            elseif row.species == "Male"
                row.species = "Kiwi"

            elseif row.species == "Female"
                row.species = "Kiwi"
            end
        end

        #println(pwd() * "\t" * levels(data_frame.species))

        # Check to see if labels exist in the data frame
        valid_labels = String[]

        for label in labels
            if !(label in levels(data_frame.species))
                message = pwd() * """ "$label" label not present"""
                println("$message")
            else
                push!(valid_labels, label)
            end
        end

        if length(valid_labels) < 1
            message = pwd() * " no valid labels"
            return message

            # If there are valid labels present construct the dataset
        else
            set = filter(:species => species -> species in valid_labels, data_frame)
            path_sets = groupby(set, :Path)

            for file_path in eachindex(path_sets)
                #print("$file_path")
                f = path_sets[file_path]
                data = Any[]
                headers = Any[
                    "Selection",
                    "View",
                    "Channel",
                    "start_time",
                    "end_time",
                    "low_f",
                    "high_f",
                    "Species",
                    "Notes",
                ]
                push!(data, headers)
                for (index, row) in enumerate(eachrow(f))
                    push!(
                        data,
                        [
                            index,
                            "Spectrogram 1",
                            "1",
                            row."Start Time (relative)",
                            row."End Time (relative)",
                            row.min_freq,
                            row.max_freq,
                            row.species,
                            "",
                        ],
                    )
                end
                p = split(f[1, :Path], ".")
                if length(p) < 3
                    # Julia does not like | in file names, but all my csv files as already built with | in file path.
                    # But I need q[1], q[2] later when I save the wav anyway, so its ok
                    q = split(p[end-1], "|")
                    r = string(q[1], "_", q[2], "_", q[3])
                    output_file =
                        "/media/david/956f2166-5055-4648-b3af-e6cfcec11297/2023-02-13_Kiwi/Annotations/" *
                        r *
                        ".Table.1.selections.txt"
                else
                    error("File names have gone to hell. One period only David")
                end
                open(output_file, "w") do io
                    writedlm(io, data, '\t')
                end
                src = chop(f[1, :File_Name], tail = 5)
                dst =
                    "/media/david/956f2166-5055-4648-b3af-e6cfcec11297/2023-02-13_Kiwi/AudioData/" *
                    q[1] *
                    "_" *
                    q[2] *
                    "_" *
                    src
                cp(src, dst, force = true)
                print(".")
            end
        end
    end
end

"""
json(csv_file::String)

Takes a csv from Finder labelling step and writes json for consumption through AviaNZ labelling GUI.

Note, if single labels are quoted csv wont read, quote multi labels only, or open with numbers then export, maybe use tsv instead of csv.
Not must be a lonely label, on a line by itself, not mixed in with other labels, watch out for ", sanitise using find and replace in numbers.
It's going to write files in folders, run in the correct Tagging subdirectory
Assumes a file duration of 895 seconds.
Writes evey label to file except "Not".
Note when I make it work on Not files I will end up with multiple identical labels if there is more than one false positive in that file.

using CSV, DataFrames, DataFramesMeta
"""
function json(csv_file::String)
    K = DataFrame(CSV.File(csv_file))
    #subset(K, :label => ByRow(label -> label != "Not")) the one below is better, it ignores white space and other labels.
    subset!(K, :label => ByRow(label -> !occursin(label, "Not")))
    sort!(K, :file)
    @transform!(K, @byrow :File = (split(:file, "-"))[5])
    @transform!(K, @byrow :S = (split(:file, "-"))[6])
    @transform!(K, @byrow :E = chop((split(:file, "-"))[7], tail = 4))
    @transform!(
        K,
        @byrow :Path =
            (split(:file, "-"))[1] *
            "/" *
            (split(:file, "-"))[2] *
            "-" *
            (split(:file, "-"))[3] *
            "-" *
            (split(:file, "-"))[4]
    )
    gdf = groupby(K, :Path)
    #p=sort((combine(gdf, nrow)), :nrow) #This gives a nice list of nrows per recorder location 
    #paths=p.Path
    paths = levels(K.Path) # more efficient than above unless I am looking
    mkpath.(paths)
    #
    for loc in gdf
        B = groupby(loc, :File)
        for group in B
            io = open("$(group.Path[1])/$(group.File[1]).WAV.data", "w")
            write(io, """[ """)
            write(io, """{"Reviewer":"D", "Operator":"Finder", "Duration":895},""")
            for row in eachrow(group)
                write(io, """[$(row.S), $(row.E), 100, 7900, """)
                x = split(row.label, ",")
                write(io, """[""")
                for label in x
                    l = filter(x -> !isspace(x), label)
                    write(
                        io,
                        """{"species":"$l", "calltype":"", "filter": "Opensoundscape-Kiwi", "certainty":99}""",
                    )
                    if label != last(x)
                        write(io, """,""")
                    end
                end
                write(io, "]")
                write(io, """]""") #check the form of json here, think its right.
                if row != last(group)
                    write(io, """,""")
                end
            end
            write(io, """ ]""")
            close(io)
            print(".")
        end
    end
    #
    println("\ndone")
end

"""
Takes dawn dusk.csv and returns a dict to be consumeed by night().
~/dawn_dusk.csv
At present it goes from first C05 recording 28/10/21 to the end of 2022
dict = construct_dawn_dusk_dict("/home/david/dawn_dusk.csv")

using CSV, DataFrames

a=CSVto.construct_dawn_dusk_dict("/media/david/SSD1/dawn_dusk.csv")
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

# Construct a date to test function

# g=DateTime("2021-11-02T21:14:35",dateformat"yyyy-mm-ddTHH:MM:SS")
CSVto.night(g, a)
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

# assumes run from Clips_xxxx-xx-xx folder and that actual_mfdn.csv, predicted_cof.csv, predicted_noise.csv, and that
# assumes file names if not specified
# saves a csv and also returns a dataframe
# using CSV, DataFrames, DataFramesMeta
function aggreagte_labels(actual="actual_mfdn.csv", cof="predicted_cof.csv", noise="predicted_noise.csv", outfile="pomona_labels.csv")
    a=DataFrame(CSV.File(actual))
    c=DataFrame(CSV.File(cof))
    rename!(c,:label => :distance)
    n=DataFrame(CSV.File(noise))
    rename!(n,:label => :noise)

    # make unique true not needed now I have renamed label column, but will help later maybe, in case of duplicate label names.
    x=leftjoin(a, c, on = :file)
    df=leftjoin(x, n, on = :file, makeunique=true)

    # location, f, box
    @transform!(df, @byrow :location = split(split(:file, "/")[2], "-")[1])
    @transform!(df, @byrow :f = split(split(:file, "/")[2], "-")[5] * ".WAV")
    @transform!(df, @byrow :box = "[$(split(split(:file, "/")[2], "-")[end-1]), $(chop(split(split(:file, "/")[2], "-")[end], tail=4))]" )

    # male, female, duet, not
    @transform!(df, @byrow @passmissing :male = split(:file, "/")[1] == "M" ? true : false)
    @transform!(df, @byrow @passmissing :female = split(:file, "/")[1] == "F" ? true : false)
    @transform!(df, @byrow @passmissing :duet = split(:file, "/")[1] == "D" ? true : false)
    @transform!(df, @byrow @passmissing :not_kiwi = split(:file, "/")[1] in ["KA", "KE", "N", "Q"] ? true : false)

    # other_label
    @transform!(df, @byrow @passmissing :other_label = split(:file, "/")[1] in ["KA", "KE", "Q"] ? split(:file, "/")[1] : missing)

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

end # module

