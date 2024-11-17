
#unfinished
using DataFrames, CSV, Glob, DataFramesMeta

function make_skraak_dataset(
    input_file::String,
    output_path::String = "/media/david/SSD2/PrimaryDataset/kiwi_set/",
)
    @info "This could take some time.\nFirst we move audio files.\nThen we make the spectrogram images."
    move_files(input_file, output_path) |> x -> save_pngs(x, output_path)
end

"""
Only moves WAVs not already there in dataset
converts WAVs to flac to save space, file metadata will not survive
requires columns :location, :file, :start_time, :end_time
:file is the file name, :location is the actual recorder location eg "C05"
run where the raw data is
will find file in folder structure location/trip_date/file
constructs dataset at output_path
assumes file name has one . for extension only
"""
function move_files(input_file::String, output_path::String)
    df = DataFrames.DataFrame(CSV.File(input_file))
    @assert nrow(df) > 0 "Empty csv therefore dataframe"
    if "box" in names(df)
        @transform!(df, @byrow :start_time = first(eval(Meta.parse(:box))))
        @transform!(df, @byrow :end_time = last(eval(Meta.parse(:box))))
    end
    for col_name in ["location", "file", "start_time", "end_time"]
        @assert col_name in names(df) "Column $col_name not present in csv"
    end
    select!(df, :location, :file, :start_time, :end_time)
    @transform!(df, @byrow :key = :location * "-" * :file)
    @info "Moving files..."
    k = levels(df.key) #Vector{String}:
    for item in k
        fldr = split(item, ".")[end-1]
        outf = replace(item, ".wav" => ".flac", ".WAV" => ".flac")
        if !isfile("$output_path$(fldr)/$outf")
            println(item)
            l, f = split(item, "-")
            b = Glob.glob("$l/*/$f")
            @assert length(b) == 1
            mkpath("$fldr")
            signal, freq = Skraak.load_audio_file(first(b))
            save("$output_path$(fldr)/$outf", signal, freq)
        end
    end
    return df
end

# work needed to make it save in correct place, currently saves relativ to pwd, this is incorrect relative to the previous function
function save_pngs(df::DataFrame, output_path::String)
    @info "$(length(levels(df.key))) files"
    @info "$(length(df.key)) labels"
    select!(df, :key, :start_time, :end_time)
    @info "Making spectrogram images..."
    gdf = DataFrames.groupby(df, :key)
    for f in gdf #where f = the file
        file = first(f.key) |> x -> replace(x, ".wav" => ".flac", ".WAV" => ".flac")
        folder = split(file, ".")[1]

        #signal, freq = WAV.wavread("kiwi_set_2023-11-13/$folder/$file")
        signal, freq = Skraak.load_audio_file("$output_path$folder/$file")
        length_signal = length(signal)
        duration = length_signal ÷ freq

        mkpath("$output_path$folder/K")
        mkpath("$output_path$folder/N")

        ldf = DataFrames.DataFrame(second = 1:duration, kiwi = false)

        #get a list of start and end times for each clip, clumsy, need to fix this bit
        kiwi = collect(map(collect, zip(f.start_time, f.end_time)))
        @info (folder, duration, kiwi)

        for clip in kiwi
            clip[1] > 0 ? st = clip[1] : st = 1
            clip[2] <= duration ? nd = clip[2] : nd = duration
            ldf.kiwi[st:nd] .= true
        end
        start = 1
        while start + 4 <= duration
            wdf = ldf[start:start+4, :]
            #make image
            st, en = calculate_clip(start, start + 4, freq, length_signal)
            sample = signal[Int(st):Int(en)]
            plot = Skraak.get_image_from_sample(sample, freq)
            if true in levels(wdf.kiwi)
                #save to K folder
                PNGFiles.save("$output_path$folder/K/$folder-$start-$(start+4).png", plot)
                start += 2
            else
                #save to N folder
                PNGFiles.save("$output_path$folder/N/$folder-$start-$(start+4).png", plot)
                start += 5
            end
        end
        if start + 4 > duration
            wdf = ldf[duration-4:duration, :]
            #make image
            st, en = calculate_clip(duration - 4, duration, freq, length_signal)
            sample = signal[Int(st):Int(en)]
            plot = Skraak.get_image_from_sample(sample, freq)
            #save to correct folder
            true in levels(wdf.kiwi) ? l = "K" : l = "N"
            PNGFiles.save(
                "$output_path$folder/$l/$folder-$(duration-4)-$duration.png",
                plot,
            )
        end
    end
end

function calculate_clip(st::Int, en::Int, freq::Int32, len::Int)
    s = (st * freq) - freq + 1
    en * freq <= len ? e = en * freq : e = len
    return s, e
end
