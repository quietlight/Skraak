# JSONto.jl

module JSONto

"""
JSONto Submodules:
airtable, kiwi_csv, mutate_call_type (not  exported),

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

export airtable, kiwi_csv

using CSV, DataFrames, Dates, DelimitedFiles, DSP, Glob, JSON3, Plots, WAV, XMLDict

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

end # module
