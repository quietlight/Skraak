# Legacy.jl

module Legacy

"""
Legacy submodules:
    airtable_buckets
    json
    dataset
    airtable
    df_old_labels
    df_new_labels
    kiwi_csv
    mutate_call_type (not  exported)
"""


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
JSONto Submodules:
    airtable
    df_old_labels
    df_new_labels
    kiwi_csv
    mutate_call_type (not  exported)

# cd to the working directory, to run:
julia
using Glob, Skraak
x = glob("*/2022-10-08")
for i in x
cd(i)
Skraak.JSONto.airtable() or Skraak.JSONto.kiwi_csv()
cd("/path/to/working/directory")
end
"""

"""
airtable()

This function takes raw AviaNZ labels in a folder and generates 
a wav, png and a csv file.

I upload to Airtable to sift through false positives.

It is looking for species=Kiwi (Tokoeka Fiordland)

output folder for wav and png is  "/home/david/Upload/"

csv file assumes I am uploading to aws

csv is written in the same folder as the JSON .data files

using Dates, DelimitedFiles, DSP, Glob, JSON3, Plots, WAV
"""

function airtable()
    data_list = glob("*.WAV.data")
    trap_location = split((cd(pwd, "..")), "/") |> last
    println("Working on: $trap_location")
    trip_date = split(pwd(), "/") |> last

    data = Any[]
    headers = Any[
        "File Name",
        "Image",
        "Audio",
        "Call Type",
        "Length (seconds)",
        "Start Time",
        "Trap Number",
    ]
    push!(data, headers)

    for f in data_list
        json_string = read(f, String)
        read_only_dict = JSON3.read(json_string)
        if length(read_only_dict) > 1
            dict = JSON3.copy(read_only_dict)
            file = chop(f, tail = 5)
            signal, freq = wavread(file)
            for h in eachindex(dict[2:end])
                for i in eachindex(dict[h+1][5])
                    if dict[h+1][5][i][:species] == "Kiwi (Tokoeka Fiordland)"
                        c_type = get(dict[(h+1)][5][i], :calltype, missing)

                        start =
                            file[1:4] *
                            "-" *
                            file[5:6] *
                            "-" *
                            file[7:8] *
                            "T" *
                            file[10:11] *
                            ":" *
                            file[12:13] *
                            ":" *
                            file[14:15]
                        #t0 = Dates.Time(start, "H:M:S")
                        t0 = DateTime(start)
                        t1 = dict[h+1][1] |> floor |> Int |> Dates.Second

                        #Make wav file
                        st = Int(floor(dict[h+1][1]))
                        if st == 0
                            st += 1
                        end
                        en = Int(ceil(dict[h+1][2]))
                        out = trap_location * "-" * chop(file, tail = 4) * "-S$st-E$en"
                        outfile = "/home/david/Upload/" * out

                        sample = signal[Int(st * freq):Int(en * freq)]
                        #wavplay(sample, Int(freq))
                        wavwrite(sample, "$outfile.wav", Fs = Int(freq))

                        #Make spectrogram
                        #n = length(sample[:,1])÷1000
                        n = 400
                        fs = convert(Int, freq)
                        S = spectrogram(sample[:, 1], n, n ÷ 200; fs = fs)
                        heatmap(
                            S.time,
                            S.freq,
                            pow2db.(S.power),
                            xguide = "Time [s]",
                            yguide = "Frequency [Hz]",
                        )
                        savefig(outfile)

                        #append to data, to become csv
                        line = [
                            out,
                            "https://label-pomona.s3-ap-southeast-2.amazonaws.com/$out.png",
                            "https://label-pomona.s3-ap-southeast-2.amazonaws.com/$out.wav",
                            c_type,
                            en - st,
                            t0 + t1,
                            "$trap_location/$trip_date",
                        ]
                        push!(data, line)
                    end
                end
            end
        end
    end

    output_file =
        "Detections-" *
        trap_location *
        "-" *
        trip_date *
        "-Processed-" *
        string(Dates.today()) *
        ".csv"

    open(output_file, "w") do io
        writedlm(io, data, '\t')
    end

    println("\n\n", "Done", "\n")
end

"""
df_old_labels()

This function takes hand labeled AviaNZ JSON and outputs a df to be put into a database table

It is in the same place the wav files are, usually a removeable drive but could be on the Linux beasts internal drive.

This function needs hand labelled .data files.

Used like:
using CSV
for folder in folders
    cd(folder)
    df = df_old_labels()
    CSV.write("/Users/davidcary/Desktop/pomona_labels_old.csv", df; append=true)
    cd("/Volumes/Pomona-2/")
end

using DataFrames, Glob, JSON3
"""

# For K_set style labels
function df_old_labels()::DataFrame
    df = DataFrame(
        location = String[],
        file = String[],
        box = Vector{Float64}[],
        K_Set = Bool[],
        Duet = Bool[],
        K_Close = Bool[],
        K_Ok = Bool[],
        K_Far = Bool[],
        K_M = Bool[],
        K_F = Bool[],
        K_MF = Bool[],
        K_UMF = Bool[],
        other_label = String[],
        noise_level = String[],
        noise_type = Vector{String}[],
    )

    data_list = glob("*.data")

    if length(data_list) == 0
        return df
    end

    raw_path_vec = split(pwd(), "/")[end-2:end]

    #disk = raw_path_vec[1]
    location = raw_path_vec[2]
    #trip_date = raw_path_vec[3]

    println("\n", raw_path_vec)

    for f in data_list
        file = chop(f, tail = 5)

        json_string = read(f, String)
        dict = JSON3.read(json_string)
        if length(dict) > 1
            noise_level = get(dict[1], "noiseLevel", missing)
            noise_type = get(dict[1], "noiseTypes", missing)

            for h in eachindex(dict[2:end])
                box = dict[(h+1)][1:4]

                row = Dict(
                    :location => location,
                    :file => file,
                    :box => box,
                    :K_Set => false,
                    :Duet => false,
                    :K_Close => false,
                    :K_Ok => false,
                    :K_Far => false,
                    :K_M => false,
                    :K_F => false,
                    :K_MF => false,
                    :K_UMF => false,
                    :other_label => "",
                    :noise_level => noise_level,
                    :noise_type => noise_type,
                )

                for i in eachindex(dict[h+1][5])
                    label = dict[(h+1)][5][i][:species]

                    if label == "K-Set"
                        row[:K_Set] = true
                    elseif label == "Duet"
                        row[:Duet] = true
                    elseif label == "K-Close"
                        row[:K_Close] = true
                    elseif label == "K-Ok"
                        row[:K_Ok] = true
                    elseif label == "K-Far"
                        row[:K_Far] = true
                    elseif label == "K-M"
                        row[:K_M] = true
                    elseif label == "K-F"
                        row[:K_F] = true
                    elseif label == "K-MF"
                        row[:K_MF] = true
                    elseif label == "K-?MF"
                        row[:K_UMF] = true
                    else
                        row[:other_label] = label
                    end
                end
                push!(df, row)
            end
        end

        print(".")
    end

    return df
end

# For New style labels
function df_new_labels()::DataFrame
    df = DataFrame(
        location = String[],
        file = String[],
        box = Vector{Float64}[],
        Male = Bool[],
        Female = Bool[],
        Close = Bool[],
        Geese = Bool[],
        Kaka = Bool[],
        Kea = Bool[],
        Listen = Bool[],
        LTC = Bool[],
        Morepork = Bool[],
        Not = Bool[],
        Plover = Bool[],
        other_label = String[],
    )

    data_list = glob("*.data")

    if length(data_list) == 0
        return df
    end

    raw_path_vec = split(pwd(), "/")[end-2:end]

    #disk = raw_path_vec[1]
    location = raw_path_vec[2]
    #trip_date = raw_path_vec[3]

    println("\n", raw_path_vec)

    for f in data_list
        file = chop(f, tail = 5)

        json_string = read(f, String)
        dict = JSON3.read(json_string)
        if length(dict) > 1

            #noise_level = get(dict[1], "noiseLevel", missing)
            #noise_type = get(dict[1], "noiseTypes", missing)

            for h in eachindex(dict[2:end])
                box = dict[(h+1)][1:4]

                row = Dict(
                    :location => location,
                    :file => file,
                    :box => box,
                    :Male => false,
                    :Female => false,
                    :Close => false,
                    :Geese => false,
                    :Kaka => false,
                    :Kea => false,
                    :Listen => false,
                    :LTC => false,
                    :Morepork => false,
                    :Not => false,
                    :Plover => false,
                    :other_label => "",
                )

                for i in eachindex(dict[h+1][5])
                    label = dict[(h+1)][5][i][:species]

                    if label == "Male"
                        row[:Male] = true
                    elseif label == "Female"
                        row[:Female] = true
                    elseif label == "Close"
                        row[:Close] = true
                    elseif label == "Geese"
                        row[:Geese] = true
                    elseif label == "Kaka"
                        row[:Kaka] = true
                    elseif label == "Kea"
                        row[:Kea] = true
                    elseif label == "Listen" || "?"
                        row[:Listen] = true
                    elseif label == "LTC"
                        row[:LTC] = true
                    elseif label == "Morepork"
                        row[:Morepork] = true
                    elseif label == "Not"
                        row[:Not] = true
                    elseif label == "Plover"
                        row[:Plover] = true
                    else
                        row[:other_label] = label
                    end
                end
                push!(df, row)
            end
        end

        print(".")
    end

    return df
end

"""
kiwi_csv()

This function takes hand labeled AviaNZ JSON and outputs a csv to  
be used by pomona.jl et al

It is in the same place the wav files are, usually a removeable drive 
but could be on the Linux beasts internal drive.

The csv must be in MacLabels for pomona.jl to find it.

This function needs the raw wav files, hand labelled .data files and a gpx.

using Dates, DelimitedFiles, Glob, JSON3, WAV, XMLDict
"""

function kiwi_csv()
    waypoint = glob("*.gpx")
    location = read(waypoint[1], String) |> xml_dict
    raw_path_vec = split(pwd(), "/")[end-1:end]
    path = raw_path_vec[1] * "|" * raw_path_vec[2] * "|"
    data = Any[]
    headers = Any[
        "Location",
        "Recording_Period_Start",
        "Recording_Period_End",
        "Latitude",
        "Longitude",
        "File_Name",
        "start",
        "end",
        "min_freq",
        "max_freq",
        "certainty",
        "call_type",
        "species",
        "Noise_Types",
        "Noise_Level",
        "Time Zone",
        "Moth ID",
        "Gain",
        "Battery",
        "Temperature",
        "Start Time (relative)",
        "End Time (relative)",
        "Path",
    ]
    push!(data, headers)
    wav_list = glob("*.WAV") |> sort
    start_recording_period =
        wav_list[1][1:4] *
        "-" *
        wav_list[1][5:6] *
        "-" *
        wav_list[1][7:8] *
        "T" *
        wav_list[1][10:11] *
        ":" *
        wav_list[1][12:13] *
        ":" *
        wav_list[1][14:15]
    finish_recording_period =
        wav_list[end][1:4] *
        "-" *
        wav_list[end][5:6] *
        "-" *
        wav_list[end][7:8] *
        "T" *
        wav_list[end][10:11] *
        ":" *
        wav_list[end][12:13] *
        ":" *
        wav_list[end][14:15]
    data_list = glob("*.WAV.data")
    for f in data_list
        # get wav metadata
        print(".")
        wav_file = chop(f, tail = 5)
        # wav_file = rstrip(f, ".data") may be better
        _, _, _, binary_metadata = wavread(wav_file)
        comment_vector = split(wav_info_read(binary_metadata)[:ICMT], " ")
        time_zone = chop(comment_vector[5], head = 1, tail = 1)
        moth_id = comment_vector[8]
        gain = comment_vector[10]
        battery = chop(comment_vector[15])
        temperature = chop(comment_vector[19], tail = 2)

        json_string = read(f, String)
        dict = JSON3.read(json_string)
        if length(dict) > 1
            nt = get(dict[1], "noiseTypes", [])
            nl = get(dict[1], "noiseLevel", missing)
            for h in eachindex(dict[2:end])
                s = f[10:11] * ":" * f[12:13] * ":" * f[14:15]
                t0 = Dates.Time(s, "H:M:S")
                st = dict[(h+1)][1]
                en = dict[(h+1)][2]
                t1 = st |> round |> Dates.Second
                t2 = en |> round |> Dates.Second

                for i in eachindex(dict[h+1][5])
                    ##beware was c_type = get(dict[(h+1)][5][i], :calltype, missing)
                    c_type = get(dict[(h+1)][5][i], :filter, missing)
                    line = [
                        location["gpx"]["wpt"]["name"],
                        start_recording_period,
                        finish_recording_period,
                        parse(Float64, (location["gpx"]["wpt"][:lat])),
                        parse(Float64, (location["gpx"]["wpt"][:lon])),
                        String(f),
                        t0 + t1,
                        t0 + t2,
                        dict[(h+1)][3],
                        dict[(h+1)][4],
                        dict[(h+1)][5][i][:certainty],
                        c_type,
                        dict[(h+1)][5][i][:species],
                        nt,
                        nl,
                        time_zone,
                        moth_id,
                        gain,
                        battery,
                        temperature,
                        st,
                        en,
                        path * wav_file,
                    ]
                    push!(data, line)
                end
            end
        end
    end

    output_file =
        "kiwi_data-" * location["gpx"]["wpt"]["name"] * "-" * string(Dates.today()) * ".csv"

    open(output_file, "w") do io
        writedlm(io, data, '\t')
    end

    println("\n\n", output_file, " written sucessfully", "\n")
end

"""
mutate_call_type()

This function mutates my K-XX labels to M ,F, MF, ? and writes a .backup 
file with the old label.

I used this to train on AviaNZ but dont need it anymore, it's here just 
in case. I can do everything I need in a dataframe now that I  am 
training with opensoundcloud 

using Dates, DelimitedFiles, Glob, JSON3, WAV, XMLDict (I think)
It was part of the kiwi_csv() function in the old days
"""

function mutate_call_type()
    data_list = glob("*.WAV.data")
    for f in data_list
        json_string = read(f, String)
        read_only_dict = JSON3.read(json_string)
        dict = JSON3.copy(read_only_dict)
        if length(dict) > 1
            for h in eachindex(dict[2:end])
                for i in eachindex(dict[h+1][5])
                    if dict[h+1][5][i][:species] == "K-M"
                        dict[h+1][5][i][:calltype] = "M"
                    elseif dict[h+1][5][i][:species] == "K-F"
                        dict[h+1][5][i][:calltype] = "F"
                    elseif dict[h+1][5][i][:species] == "K-MF"
                        dict[h+1][5][i][:calltype] = "MF"
                    elseif dict[h+1][5][i][:species] == "K-?MF"
                        dict[h+1][5][i][:calltype] = "?"
                    end
                end
            end
        end

        backup_file = String(f) * ".backup"
        open(backup_file, "w") do io
            JSON3.write(io, read_only_dict)
        end

        open(f, "w") do io
            JSON3.write(io, dict)
        end
    end
end

"""
clip()

This function takes a preds.csv files and generates
file names, wav's, spectrograms etc to be reviewed.
It returns a dataframe which can be piped into airtable_buckets()
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

    dawn_dusk_dict = Utility.construct_dawn_dusk_dict("/media/david/SSD1/dawn_dusk.csv")
    pres_night = @subset(
        pres,
        @byrow night(
            :DateTime,
            dawn_dusk_dict,
        )
    )
     
    files = groupby(pres_night, :file)
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


end # module