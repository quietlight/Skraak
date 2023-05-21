module Skraak

export aggreagte_labels, clip, dataset, Utility

include("Utility.jl")

"""
Skraak functions:
    clip
    dataset
    aggreagte_labels

Skraak submodules:
	Utility
	#Legacy#
"""

using   CSV,
        DataFrames,
        DataFramesMeta,
        Dates,
        DSP,
        Glob,
        HTTP,
        JSON,
        Plots,
        Random,
        TimeZones,
        WAV


"""
clip()

This function takes a preds.csv files and generates
file names, wav's, spectrograms etc to be reviewed.
it calls night() therefore night() must be available.

It should be run from Pomona-1/ or Pomona-2/, assumes it is, it uses the path
It saves  wav and png files to /home/david/Upload/

using Glob, Skraak
predictions = glob("path/to/preds*")
for file in predictions
	clip(file)
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
        @byrow night(:DateTime, dawn_dusk_dict)
    	)

    files = groupby(pres_night, :file)
    
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

                savefig(outfile)
                
            end
        end
        print(".")
    end
    println("\ndone $location/$trip_date \n")
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
img_dataset(df::DataFrame)

Takes a dataframe and makes png spectro images for secondary classifier.
Should be run from /media/david


using DSP, Plots, WAV, DataFrames, CSV, Glob

"""
function img_dataset(df::DataFrame)
    for row in eachrow(df)
        signal, freq = wavread("$(row.drive)/$(row.drive)/$(row.location)/$(row.trip_date)/$(row.file)")
        row.box[1] * freq > 1 ? st = floor(Int, (row.box[1] * freq)) : st = 1
        row.box[2] * freq < length(signal) ? en = ceil(Int, (row.box[2] * freq)) : en = length(signal)
        sample = signal[Int(st):Int(en)]
        name = "$(row.location)-$(row.trip_date)-$(chop(row.file, tail=4))-$(Int(floor(row.box[1])))-$(Int(ceil(row.box[2])))"
        outfile = "/home/david/ImageSet/$(row.label)/$name"
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
        savefig(outfile)
        print(".")
    end
    println("done")
end



"""
aggreagte_labels(actual="actual_mfdn.csv", cof="predicted_cof.csv", noise="predicted_noise.csv", outfile="pomona_labels.csv")

This function prepares the csv output from my  hand classification and secondary models and ouputs a df, and csv for insertion into AudioData.duckdb using the duckdb cli or using DFto.audiodata_db()

assumes run from Clips_xxxx-xx-xx folder and that actual_mfdn.csv, predicted_cof.csv, predicted_noise.csv, and that
assumes file names if not specified
saves a csv and also returns a dataframe

using CSV, DataFrames, DataFramesMeta
"""
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
