# CSVto.jl

module CSVto

"""
CSVto submodules:
    dataset
    airtable


"""

export dataset, airtable

using Glob, CSV, DataFrames, Dates, DelimitedFiles

"""
dataset()

This function takes a csv file in a folder and generates a dataset 
with label K-Set. 

It works on CSV's from hand labelled AvianNZ style JSON.

It saves  wav files to /media/david/72CADE2ECADDEDFB/DataSet/$label/  
and annotation files to a seperate directory as raven files. (it would be 
better for me to just build the csv direct, soon.)

    # cd to the working directory, to run:
    julia
    using Glob, Skraak
    x = glob("*/2022-10-08")
    for i in x
    cd(i)
    Skraak.CSVto.dataset()
    cd("/path/to/working/directory")
    end

using Glob, CSV, DataFrames, DelimitedFiles
"""

function dataset()
    raw_data = glob("*.csv")

    if length(raw_data) == 1
        data_frame = DataFrame(CSV.File(raw_data[1]))
    else
        message = pwd() * " has no csv present, or more than 1"
        return message
    end

    if !("K-Set" in levels(data_frame.species))
        message = pwd() * " K-Set not present"
        return message
    else

        species_grouped_frames = groupby(data_frame, :species)
        # Beware this comma!!
        kset = species_grouped_frames[(species = :"K-Set",)]
        path_sets = groupby(kset, :Path)

        for file_path in eachindex(path_sets)
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
                    "/media/david/72CADE2ECADDEDFB/DataSet/K-Set_AnnoTables/" *
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
                "/media/david/72CADE2ECADDEDFB/DataSet/K-Set/" *
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

"""
airtable()

This function takes a preds.csv files and generates 
file names, wav's, spectrograms etc to be uploaded to airtable for review.
It returns a dataframe to be piped into airtable_buckets()
it calls night() therefore night() must be available.

It should be run from Pomona-1/ or Pomona-2/
It saves  wav and png files to /home/david/Upload/ 
It returns a dataframe to be piped into airtable_buckets()

for file in predictions
    airtable_buckets(airtable(file))
end

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

using Glob, CSV, DataFrames, DataFramesMeta, Dates, DSP, Plots, Random, WAV
"""

function airtable(file::String)
    # Assumes function run from Pomona-1 or Pomona-2
    location, trip_date, _ = split(file, "/")
    data = DataFrame(CSV.File(file))
    @transform!(data, @byrow :DateTime = DateTime(chop(:file, head=2, tail=4), dateformat"yyyymmdd_HHMMSS"))
    gdf = groupby(data, :present)
    pres = gdf[(present=1,)]
    pres_night = @subset(pres, @byrow night(:DateTime, dict)) #throw away nights
    #sort!(pres_night)
    files = groupby(pres_night, :file)
    #airtable = DataFrame(a = Any[], b = Any[])
    airtable = DataFrame(FileName=String[], Image=String[], Audio=String[], StartTime=DateTime[], Length=Float64[], Location=String[], Trip=String[])
    for (k,v) in pairs(files)
        file_start_time = v.DateTime[1]
        file_name = chop(v.file[1], head=2, tail=4)
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
        detections=filter(x -> length(x) > 1, s)
        #println(file_name, file_start_time, detections)
        if length(detections) > 0
            #load file
            signal, freq = wavread("$location/$trip_date/$file_name.WAV")
            for detection in detections
                #if the detection starts at start of the file I am cuttiing the first 0.1 seconds off.
                (first(detection)-2)*freq >= 0 ? st = (first(detection)-2)*freq : st = 1
                (last(detection)+7)*freq <= length(signal) ? en = (last(detection)+7)*freq : en = length(signal)
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
                    xguide = "Time [s]",
                    yguide = "Frequency [Hz]",
                )
                savefig(outfile)
                #push to to airtable df
                push!(airtable, [name, "https://label-pomona.s3-ap-southeast-2.amazonaws.com/$name.png", "https://label-pomona.s3-ap-southeast-2.amazonaws.com/$name.wav", file_start_time, (en-st)/freq, location, trip_date])
                
            end
        end
        print(".")
        #println(k, v)
    end
    return airtable
end

"""
airtable_buckets(dataframe)

Takes a dataframe with columns Audio, Trip, FileName, Image, Length, StartTime, Location, and returns json in ~/Airtable to be uploaded to airtable.
Intended to work with airtable() in a chain
"""
function airtable_buckets(dataframe)
	e=floor(nrow(dataframe)/10)
	f=round((nrow(dataframe)/10-e)*10)
	g=[]
	for i in 1:e
		for h in 1:10
			push!(g, i)
		end
	end
	for j in 1:f
		push!(g, e+1)
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
    end;

end

end  # module