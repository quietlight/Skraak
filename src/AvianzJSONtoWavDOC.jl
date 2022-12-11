#!/usr/bin/env julia

#=
#check I have not edited anything out of the script first
 
using Glob

cd("/media/david/72CADE2ECADDEDFB/PomonaData/LinuxLabels")
a = glob("*/*/")

for i in a
 cd(i)
 include("/media/david/72CADE2ECADDEDFB/PomonaData/MakeWavPngCsv/AvianzJSONtoWavPngCsv.jl")
 cd("/media/david/72CADE2ECADDEDFB/PomonaData/LinuxLabels")
end

#For Secretary Island
using Glob

cd("/media/david/72CADE2ECADDEDFB/DOC/LinuxLabels")
a = glob("*/*/")

for i in a
 cd(i)
 include("/media/david/72CADE2ECADDEDFB/PomonaData/MakeWavPngCsv/AvianzJSONtoWavDOC.jl")
 cd("/media/david/72CADE2ECADDEDFB/DOC/LinuxLabels")
end
=#

# cd to the working directory, to run:
# julia /media/david/72CADE2ECADDEDFB/PomonaData/MakeWavPng/AvianzJSONtoWavPlusPng.jl


using Dates, DelimitedFiles, DSP, Glob, JSON3, Plots, WAV

function AvianzJSONtoWavPlusPng()
    data_list = glob("*.wav.data")
    println(data_list)
    trap_location = split((cd(pwd, "..")), "/") |> last
    trip_date = split(pwd(), "/") |> last
    
    data = Any[]
    headers = Any[
        "File Name",
        "Image",
        "Audio",
        "Call Type",
        "Length (seconds)",
        "Start Time",
        "Trap Number"
    ]
    push!(data, headers)

    for f in data_list
        json_string = read(f, String)
        read_only_dict = JSON3.read(json_string)
        if length(read_only_dict) > 1
            dict = JSON3.copy(read_only_dict)
            file = chop(f, tail=5)
            signal, freq = wavread(file)
            for h in eachindex(dict[2:end])
				
                for i in eachindex(dict[h+1][5])
                    if dict[h+1][5][i][:species] == "Kiwi (Tokoeka Fiordland)"
                        c_type = get(dict[(h+1)][5][i], :calltype, missing)

                        start = "20" *
                                file[5:6] *
                                "-" *
                                file[3:4] *
                                "-" *
                                file[1:2] *
                                "T" *
                                file[8:9] * 
                                ":" * 
                                file[10:11] * 
                                ":" * 
                                file[12:13]
                        #t0 = Dates.Time(start, "H:M:S")
                        t0 = DateTime(start)
                        t1 = dict[h+1][1] |> floor |> Int |> Dates.Second

                        #Make wav file
                        st = Int(floor(dict[h+1][1]))
                        if st == 0
                            st += 1
                        end
                        en = Int(ceil(dict[h+1][2]))
                        out = trip_date * "-" * chop(file, tail=4) * "-S$st-E$en"
                        outfile = "/home/david/Upload/" * out
                   
						sample = signal[Int(st*freq):Int(en*freq)]
                        #wavplay(sample, Int(freq))
                        wavwrite(sample, "$outfile.wav", Fs=Int(freq))

                        #Make spectrogram
                        #n = length(sample[:,1])÷1000
                        n=400
                        fs = convert(Int, freq)
                        S = spectrogram(sample[:,1], n, n÷200; fs=fs)
                        heatmap(S.time, S.freq, pow2db.(S.power), xguide="Time [s]", yguide="Frequency [Hz]")
                        savefig(outfile)

                        #append to data, to become csv
                        line =
                        [
                            out,
                            "https://label-pomona.s3-ap-southeast-2.amazonaws.com/$out.png",
                            "https://label-pomona.s3-ap-southeast-2.amazonaws.com/$out.wav",
                            c_type,
                            en - st,
                            t0 + t1,
                            "$trap_location/$trip_date"
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
AvianzJSONtoWavPlusPng()