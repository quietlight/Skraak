using WAV, DSP, Images, ThreadsX, Dates, DataFrames, CSV, Flux, CUDA, Metalhead, JLD2

device = Flux.CUDA.functional() ? gpu : cpu

function get_image_from_sample(sample, f)
	S = DSP.spectrogram(sample, 400, 2; fs = f)
	i = S.power
	if minimum(i) == 0.0
		l = i |> vec |> unique |> sort
		replace!(i, 0.0 => l[2])
	end
	image = DSP.pow2db.(i) |>
		x -> x .+ abs(minimum(x)) |>
		x -> x ./ maximum(x) |>
		x -> RGB.(x) |>
		x -> imresize(x, 224, 224)
	return image
end

function get_images_from_wav(file::String, increment::Int=5, divisor::Int=2) #5s sample, 2.5s hop
	signal, freq = wavread(file)
	if freq > 16000.0f0
		signal = DSP.resample(signal, 16000.0f0/freq; dims=1)
		freq = 16000.0f0
	end
	f = convert(Int, freq)
	inc = increment * f
	hop = f * increment ÷ divisor #need guarunteed Int, maybe not anymore, refactor
	split_signal = DSP.arraysplit(signal[:, 1], inc, hop)
	raw_images = ThreadsX.map(x -> get_image_from_sample(x, f), split_signal)
	n_samples = length(raw_images)
	return raw_images, n_samples
end

function get_images_time_from_wav(file::String)
	raw_images, n_samples = get_images_from_wav(file::String, increment, hop)
	images = images_to_tensor(raw_images)

	start_time = 0:2.5:(n_samples)-1)*2.5
	end_time = 5:2.5:(n_samples)+1)*2.5
	time = collect(zip(start_time, end_time))
	return images, time
end

function images_to_tensor(raw_images)
	images = hcat(raw_images...) |>
		x -> reshape(x, (224, 224, length(raw_images))) |>
		channelview |>
		x -> permutedims(x, (2, 3, 1, 4)) |>
		x -> Float32.(x)
	return images
end

function predict_file(file::String, folder::String, model)
	#check form of opensoundscape preds.csv and needed by my make_clips
	@time images, time = get_images_time_from_wav(file)
	data = Flux.DataLoader(data; batchsize=size(i)[4], shuffle=false) |> device
 	@time predictions = Flux.onecold(model(data)) |> cpu #######????????
 	f = (repeat(["$file"], length(time)))
	df=DataFrame(:file=>f, :start_time=>first.(t), :end_time=>last.(t), :label=>l)
	return df
end

function predict_folder(folder::String, model)
	files=glob("$folder/*.[W,w][A,a][V,v]")
	df = DataFrame(file=String[], start_time=Float64[], end_time=Float64[], kiwi=Int[]) #just so I know what they are
	CSV.write("$folder/preds-K1-2-$(today()).csv", df)
	for file in files 
		df=predict_file(file, folder, model)
		CSV.write("$folder/preds-K1-2-$(today()).csv", df, append=true)
		@info "saved $file preds to $folder/preds-K1-2-$(today()).csv"
	end
end

function load_model(model_path::String)
	model_state = JLD2.load(model_path, "model_state")
	m = Metalhead.ResNet(18, pretrain = false).layers
	# BEWARE NUMBER CLASSES
	model = Chain(m[1], AdaptiveMeanPool((1, 1)), Flux.flatten, Dense(512 => 2))
	Flux.loadmodel!(model, model_state);
end

function predict(glob_pattern::String, model)
	model = load_model(model) |> device
	folders = glob(glob_pattern)
	for folder in folders
		predict_folder(folder, model)
	end
end
#= this works, runs on gpu but requires pre made images, nearly 8 million for C05 alone, inference time is in the ball park

import Base: length
import Base: getindex
using Images
using Flux
using Metalhead
using Glob
using BSON
using DataFrames, CSV
using DataFramesMeta

imgs = glob("SSD1/test/C05/2023-09-11/*/*.png")

device = Flux.CUDA.functional() ? gpu : cpu

struct ValidationImageContainer{T<:Vector}
    img::T
end

data = ValidationImageContainer(imgs)

length(data::ValidationImageContainer) = length(data.img)

const im_size = (224, 224)

function getindex(data::ValidationImageContainer{Vector{String}}, idx::Int)
    path = data.img[idx]
    img = Images.load(path) |>
    		x -> channelview(float32.(x)) |>
            x -> permutedims(x, (3, 2, 1))
    return img, path
end
			#x -> Images.imresize(x, 224, 224) |> should already be 224x224
            #x -> collect(channelview(float32.(RGB.(x)))) |> dont need RGB, already rgb
    		#x -> collect(channelview(float32.(x))) |> why collect?

# define DataLoaders
const batch_size = 64

deval = Flux.DataLoader(
    ValidationImageContainer(imgs);
    batchsize=batch_size,
    collate = true,
    parallel = true,
)
device == gpu ? deval = Flux.CuIterator(deval) : nothing

BSON.@load "/media/david/SSD2/model_K1-2_CPU_epoch-7-0.9968-2023-09-28T06:50:04.328.bson" model
model = model |> device

function eval_f(m, d)
    pred = []
    path = []
    for (x, pth) in d
        p = Flux.onecold(m(x))
        append!(pred, p)
        append!(path, pth)
    end
    return pred, path
end


@time preds, files  = eval_f(model, deval)

df = DataFrame(file=files, label=preds)
@transform!(df, @byrow :file=replace(:file, "SSD1/test/C05/2023-09-11" => "."))
@transform!(df, @byrow :label = :label == 2 ? 0.0 : 1.0)
@rename! df :"1.0"=:label
@transform!(df, @byrow :start_time=split(:file, "/")[end] |> x -> chop(x, head=0, tail=4) |> x -> parse(Float64, x)/10)
@transform!(df, @byrow :end_time=:start_time+5.0)
@transform!(df, @byrow :file=replace(:file, r"/(\d+)\.png" => ".WAV"))
@select!(df, :file, :start_time, :end_time, :"1.0")
sort!(df)
CSV.write("/media/david/SSD1/preds-K1-2-2023-09-28_C05.csv", df)
#
@transform!(df4, @byrow :key=:file * "-" * "$(:start_time)")
@transform!(df3, @byrow :key=:file * "-" * "$(:start_time)")
df5=outerjoin(df3, df4, on = :key, makeunique=true)
CSV.write("/media/david/SSD1/join.csv", df5)
#



#
Load model first

predict_folder("folder", model)
This is slow
===================================
#
using CSV, DataFrames, DSP, Glob, Images, GLMakie, PNGFiles, WAV, Flux, Metalhead, BSON, ProgressMeter, CUDA, ThreadsX #, JLD2, Plots

BSON.@load "/media/david/SSD2/model_K1-2_CPU_epoch-7-0.9968-2023-09-28T06:50:04.328.bson" model
model |> gpu

a=glob("C05/2023-09-11/")
for folder in a
    predict_folder(folder, model)
end

function predict_folder(folder::String, model)
	files=glob("$folder/*.[W,w][A,a][V,v]")
	#check form of opensoundscape preds.csv and needed by my make_clips
	f=[]
	t=[]
	l=[]
	@showprogress for file in files ######
		@info file
		images, time = get_images_from_wav(file)
		images |> gpu
	 	@time predictions = Flux.onecold(model(images))
	 	append!(t, time)
	 	append!(l, predictions)
	 	append!(f, (repeat(["$file"], length(time))))
	end
	df=DataFrame(:file=>f, :start_time=>first.(t), :end_time=>last.(t), :label=>l)
	CSV.write("$folder/preds-K1-2-2023-09-28_GLMakie.csv", df)
	@info "saved $folder/preds-K1-2-2023-09-28_GLMakie.csv"
end

#efficient, gray but RGB
#using TerminalExtensions to see image
function get_image_from_sample(st, en, signal, f)
	sample = signal[st:en]
	S = DSP.spectrogram(sample[:, 1], 400, 2; fs = f)
	image=DSP.pow2db.(S.power) |>
		x -> LinearAlgebra.normalize(x) |>
		x -> RGB.(x) |>
		x -> imresize(x, 224, 224)
	return image
end


# new, now ThreadsX works its faster. converting to float32 makes inference faster
function get_images_from_wav(file::String)
	signal, freq = wavread(file)
	if freq > 16000.0f0
		signal = DSP.resample(signal, 16000.0f0/freq; dims=1)
		freq = 16000.0f0
	end
	length_signal = length(signal)
	f=convert(Int, freq)
	increment = 5*f-1
	hop = f*5÷2 #need guarunteed Int
	start=1
	time = []
	chop = []
	while start + increment <= length_signal
		push!(time, ((start - 1)/f, (start + increment)/f))
		push!(chop, (start, (start + increment)))
		start += hop
	end
	@time raw_images = ThreadsX.map(x -> get_image_from_sample(first(x), last(x), signal, f), chop)
	images = hcat(raw_images...) |>
		x -> reshape(x, (224, 224, length(raw_images))) |>
		channelview |>
		x -> permutedims(x, (2, 3, 1, 4)) |>
		x -> Float32.(x)

	return images, time
end


# old: non map function. converting to float32 makes inference faster
function get_images_from_wav(file::String)
	signal, freq = wavread(file)
	f=convert(Int, freq)
	if f > 16000
		signal = DSP.resample(signal, 16000/f; dims=1)
		f = 16000
	end
	length_signal = length(signal)
	increment = 5*f-1
	hop = f*5÷2 #need guarunteed Int

	raw_images = [] #in form to pass model
	time = []

	start=1
	@time while start + increment <= length_signal
	get_image_from_sample(start, (start + increment), signal, f) |>
		x -> push!(raw_images, x)
	push!(time, ((start - 1)/f, (start + increment)/f))
	start += hop
	end

	images = hcat(raw_images...) |>
		x -> reshape(x, (224, 224, length(raw_images))) |>
		channelview |>
		x -> permutedims(x, (2, 3, 1, 4)) |>
		x -> Float32.(x)

	return images, time
end


# uses Plots, slow slow NO
function get_image_from_sample(st, en, signal, f)
	sample = signal[st:en]
	S = DSP.spectrogram(sample[:, 1], 400, 2; fs = f)
	plot=Plots.heatmap(S.time, S.freq, pow2db.(S.power), size = (225, 225), showaxis = false, ticks = false, legend = false, thickness_scaling = 0 );
	buffer = PipeBuffer()
	Plots.png(plot, buffer) #Slow, slow, slow!
	image = PNGFiles.load(buffer)
	size(image) == (224, 224) ? nothing : image = Images.imresize(image, 224, 224)
	return image
end

#using GlMakie, also slow
function get_image_from_sample(st, en, signal, f)
	sample = signal[st:en]
	S = DSP.spectrogram(sample[:, 1], 400, 2; fs = f)
	f = GLMakie.Figure(resolution = (224, 224), figure_padding = 0)
	ax = GLMakie.Axis(f[1, 1], spinewidth=0)
	GLMakie.hidedecorations!(ax)
	GLMakie.heatmap!(ax, (DSP.pow2db.(S.power))', colormap = :inferno)
	buffer = PipeBuffer()
	GLMakie.show(buffer, MIME"image/png"(), f)
	image = PNGFiles.load(buffer)
	return image
end

=#