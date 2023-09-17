#=
Load model first

predict_folder("folder", model)
=#
using DataFrames, DSP, Glob, Images, Plots, PNGFiles, WAV, Flux, Metalhead, BSON

device = Flux.CUDA.functional() ? gpu : cpu

BSON.@load "/Volumes/SSD1/SecondaryModels/Working_2023-08-30/model_MFDN3_2.9067-2023-09-07T11:02:18.773.bson" model_temp
model = model_temp |> device

function predict_folder(folder::String, model)::DataFrame
	files=glob("$folder/*.[W,w][A,a][V,v]")
	#check form of opensoundscape preds.csv and needed by my make_clips
	f=[]
	t=[]
	l=[]
	for file in files
		images, time = get_images_from_wav(file)
	 	predictions = Flux.DataLoader(images, batchsize=64, shuffle=false) |> 
	 		device |>
	 		x -> predict_file(x, model)
	 	append!(f, repeat(["$file"], length(time)))
	 	append!(t, time)
	 	append!(l, predictions)
	end
	df=DataFrame(file=f, time=t, label=l)
	CSV.write("$folder/preds.csv", df)
	return df
end

function predict_file(d, m)
	pred = []
    for x in d
        p = Flux.onecold(m(x))
        append!(pred, p)
    end
    return pred
end

function get_images_from_wav(file::String)
	signal, freq = wavread(file)
	length_signal = length(signal)
	f=convert(Int, freq)
	increment = 5*f-1
	hop = f*5÷2 #need guarunteed Int

	raw_images = [] #in form to pass model
	time = []
	
	start=1
	while start + increment <= length_signal
	get_image_from_sample(start, (start + increment), signal) |>
		x -> push!(raw_images, x)
	push!(time, ((start - 1)/f, (start + increment)/f))
	start += hop
	end
	
	images = hcat(raw_images...) |> x -> reshape(x, (224, 224, length(raw_images))) |> channelview |> x -> permutedims(x, (2, 3, 1, 4))

	return images, time
end

function get_image_from_sample(st, en, signal)
	sample = signal[st:en]
	S = DSP.spectrogram(sample[:, 1], 400, 2; fs = f)
	plot=Plots.heatmap(S.time, S.freq, pow2db.(S.power), size = (225, 225), showaxis = false, ticks = false, legend = false, thickness_scaling = 0 );
	buffer = PipeBuffer()
	Plots.png(plot, buffer) #Slow, slow, slow!
	image = PNGFiles.load(buffer) |>
		x -> Images.imresize(x, 224, 224) #|>
		#x -> collect(channelview(float32.(RGB.(x)))) |> ???RGB???
		#x -> permutedims(x, (3, 2, 1))
	return image
end