#!/usr/bin/env julia

# cd to the working directory, to run:
# julia /Volumes/Pomona-1/PomonaData/ParseJSONtoPomonaKiwiCSV/AvianzJSONtoPomonaKiwiCSV_DOC.jl
# julia /media/david/72CADE2ECADDEDFB/PomonaData/ParseJSONtoPomonaKiwiCSV/AvianzJSONtoPomonaKiwiCSV_DOC.jl

using Dates, DelimitedFiles, Glob, JSON3, XMLDict

function ParseJSONtoCSV()
    #waypoint = glob("*.gpx")
    #location = read(waypoint[1], String) |> xml_dict
    raw_path_vec = split(pwd(), "/")[end-1:end]
    path = raw_path_vec[1] * "|" * raw_path_vec[2] * "|" 
    data = Any[]
    headers = Any[
        "Location",
        #"Recording_Period_Start",
        #"Recording_Period_End",
        #"Latitude",
        #"Longitude",
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
        "Start Time (relative)",
        "End Time (relative)",
        "Path"
    ]
    push!(data, headers)
    #=wav_list = glob("*.WAV") |> sort
    start_recording_period =
        wav_list[1][1:4] * "-" *
        wav_list[1][5:6] * "-" *
        wav_list[1][7:8] * "T" *
        wav_list[1][10:11] * ":" *
        wav_list[1][12:13] * ":" *
        wav_list[1][14:15]
    finish_recording_period =
        wav_list[end][1:4] * "-" *
        wav_list[end][5:6] * "-" *
        wav_list[end][7:8] * "T" *
        wav_list[end][10:11] * ":" *
        wav_list[end][12:13] * ":" *
        wav_list[end][14:15]=#
    data_list = glob("*.wav.data")
    for f in data_list
        print(".")
        wav_file = chop(f, tail=5)
        json_string = read(f, String)
        dict = JSON3.read(json_string)
        if length(dict) > 1
            nt = get(dict[1], "noiseTypes", [])
            nl = get(dict[1], "noiseLevel", missing)
            for h in eachindex(dict[2:end])

                s = f[8:9] * ":" * f[10:11] * ":" * f[12:13]
                t0 = Dates.Time(s, "H:M:S")
                st = dict[(h+1)][1]
                en = dict[(h+1)][2]
                t1 = dict[(h+1)][1] |> round |> Dates.Second
                t2 = dict[(h+1)][2] |> round |> Dates.Second

                for i in eachindex(dict[h+1][5])
                    ##beware was c_type = get(dict[(h+1)][5][i], :calltype, missing)
                    c_type = get(dict[(h+1)][5][i], :filter, missing)
                    line =
                        [
                            #location["gpx"]["wpt"]["name"],
                            split(pwd(), "/")[end],
                            #start_recording_period,
                            #finish_recording_period,
                            #parse(Float64, (location["gpx"]["wpt"][:lat])),
                            #parse(Float64, (location["gpx"]["wpt"][:lon])),
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
                            st,
                            en,
                            path * wav_file
                        ]
                    push!(data, line)
                end

            end
        end
    end

    output_file =
        "kiwi_data-" *
        #location["gpx"]["wpt"]["name"] *
        split(pwd(), "/")[end] *
        "-" *
        string(Dates.today()) * ".csv"

    open(output_file, "w") do io
        writedlm(io, data, '\t')
    end

    println("\n\n", output_file, " written sucessfully", "\n")
end
#=
function MutateCallTypeInJSON()
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

#MutateCallTypeInJSON()=#
ParseJSONtoCSV()