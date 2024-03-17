# Predict.jl

export predict

using WAV,
    DSP, Images, ThreadsX, Dates, DataFrames, CSV, Flux, CUDA, Metalhead, JLD2, FLAC, Glob
import Base: length, getindex

"""
predict(glob_pattern::String, model::String)

This function takes a glob pattern for folders (or a vector of folders) to run over, and a model path. It saves results in a csv in each folder, similar to opensoundscape

Args:

•  glob pattern (folder/) or a vector of folders
•  model path

Returns: Nothing - This function saves csv files.

I use this function to find kiwi from new data gathered on a trip. And to predict D/F/M/N for images clipped from primary detections.

It works on both audio (wav or flac) and png images.

Note:
From Pomona-3/Pomona-3/
julia -t 4
Dont forget temp environment: ] activate --temp

Use like:
using Skraak
glob_pattern = "*/*/"
model = "/media/david/SSD2/PrimaryDataset/model_K1-9_original_set_CPU_epoch-7-0.9924-2024-03-05.jld2"
predict(glob_pattern, model)
"""

function predict(glob_pattern::String, model::String)
    model = load_model_pred(model) |> device
    folders = glob(glob_pattern)
    @info "Folders: $folders"
    for folder in folders
        @info "Working on: $folder"
        predict_folder(folder, model)
    end
end

function predict(folders::Vector{String}, model::String)
    model = load_model_pred(model) |> device
    @info "Folders: $folders"
    for folder in folders
        @info "Working on: $folder"
        predict_folder(folder, model)
    end
end

#~~~~~ The guts ~~~~~#
# see load_model() from train, different input types
function load_model_pred(model_path::String)
    model_state = JLD2.load(model_path, "model_state")
    model_classes = length(model_state[1][2][1][3][2])
    @info "Model classes: $model_classes"
    f = Metalhead.ResNet(18, pretrain = false).layers
    l = Flux.Chain(AdaptiveMeanPool((1, 1)), Flux.flatten, Dense(512 => model_classes))
    model = Flux.Chain(f[1], l)
    Flux.loadmodel!(model, model_state)
    return model
end

#=
function load_bson(model_path::String)
    BSON.@load model_path model
end
=#

function predict_folder(folder::String, model)
    wav = glob("$folder/*.[W,w][A,a][V,v]")
    flac = glob("$folder/*.flac")
    audio_files = vcat(wav, flac) #if wav and flac both present will predict on all
    png_files = glob("$folder/*.png")
    #it will predict on images when both images and audio present
    if isempty(png_files)
        length(audio_files) > 0 ? predict_audio_folder(audio_files, model, folder) : @info "No png, flac, wav, WAV files present in $folder"
    else
        predict_image_folder(png_files, model, folder)
    end
end

device = CUDA.functional() ? gpu : cpu

# Predict from png images
struct PredictImageContainer{T<:Vector}
    img::T
end

length(data::PredictImageContainer) = length(data.img)

function getindex(data::PredictImageContainer{Vector{String}}, idx::Int)
    path = data.img[idx]
    img =
        #! format: off
        Images.load(path) |>
        x -> Images.imresize(x, 224, 224)|>
        x -> Images.RGB.(x) |>
        x -> collect(channelview(float32.(x))) |>
        x -> permutedims(x, (3, 2, 1))
        #! format: on
    return img, path
end

function predict_image_folder(png_files::Vector{String}, model, folder::String)
    l = length(png_files)
    @assert (l > 0) "No png files present in $folder"
    @info "$(l) png_files in $folder"
    save_path = "$folder/preds-$(today()).csv"
    loader = png_loader(png_files)
    @time preds, files = predict_pngs(model, loader)
    f = split.(files, "/") |> x -> last.(x)
    df = DataFrame(file = f, label = preds)
    CSV.write("$save_path", df)
end

function png_loader(png_files::Vector{String})
    loader = Flux.DataLoader(
        PredictImageContainer(png_files);
        batchsize = 64,
        collate = true,
        parallel = true,
    )
    device == gpu ? loader = CuIterator(loader) : nothing
    return loader
end

function predict_pngs(m, d)
    @info "Predicting..."
    pred = []
    path = []
    for (x, pth) in d
        p = Flux.onecold(m(x))
        append!(pred, p)
        append!(path, pth)
    end
    return pred, path
end

# Predict from audio files
function predict_audio_folder(audio_files::Vector{String}, model, folder::String)
    l = length(audio_files)
    @assert (l > 0) "No wav or flac audio files present in $folder"
    @info "$(l) audio_files in $folder"
    df = DataFrame(
        file = String[],
        start_time = Float64[],
        end_time = Float64[],
        label = Int[],
    )
    save_path = "$folder/preds-$(today()).csv"
    CSV.write("$save_path", df)
    for file in audio_files
        df = predict_audio_file(file, model)
        CSV.write("$save_path", df, append = true)
    end
end

function predict_audio_file(file::String, model)
    #check form of opensoundscape preds.csv and needed by my make_clips
    @info "File: $file"
    @time data = audio_loader(file)
    pred = []
    time = []
    @time for (x, t) in data
        p = Flux.onecold(model(x))
        append!(pred, p)
        append!(time, t)
    end
    f = (repeat(["$file"], length(time)))
    df = DataFrame(
        :file => f,
        :start_time => first.(time),
        :end_time => last.(time),
        :label => pred,
    )
    sort!(df)
    return df
end

function audio_loader(file::String, increment::Int = 5, divisor::Int = 2)
    raw_images, n_samples = get_images_from_audio(file::String, increment, divisor)
    images = reshape_images(raw_images, n_samples)

    # Start time and end time for each 5s audio clip, in seconds relative to the start of the file.
    start_time = 0:(increment/divisor):(n_samples-1)*(increment/divisor)
    end_time = increment:(increment/divisor):(n_samples+1)*(increment/divisor)
    time = collect(zip(start_time, end_time))

    loader = Flux.DataLoader((images, time), batchsize = n_samples, shuffle = false)
    device == gpu ? loader = CuIterator(loader) : nothing #check this works with gpu
    return loader
end

function reshape_images(raw_images, n_samples)
    images =
        #! format: off
        hcat(raw_images...) |>
        x -> reshape(x, (224, 224, 3, n_samples))
        #! format: on
    return images
end

#= not needed
function get_image_for_inference(sample, f)
    image =
        #! format: off
        get_image_from_sample(sample, f) |>
        # x -> collect(channelview(float32.(x))) |> 
        x -> permutedims(x, (3, 2, 1))
        #! format: on
    return image
end
=#

# need to change divisor to a overlap fraction, chech interaction with audioloader()
# if divisor is 0, then no overlap atm
function get_images_from_audio(file::String, increment::Int = 5, divisor::Int = 2) #5s sample, 2.5s hop
    signal, freq = load_audio_file(file)
    if freq > 16000
        signal, freq = resample_to_16000hz(signal, freq)
    end
    f = convert(Int, freq)
    inc = increment * f
    #hop = f * increment ÷ divisor #need guarunteed Int, maybe not anymore, refactor
    hop = f * increment / divisor |> x -> x == Inf ? 0 : trunc(Int, x)
    split_signal = DSP.arraysplit(signal[:, 1], inc, hop)
    raw_images = ThreadsX.map(x -> get_image_from_sample(x, f), split_signal)
    n_samples = length(raw_images)
    return raw_images, n_samples
end

function load_audio_file(file::String)
    ext = split(file, ".")[end]
    @assert ext in ["WAV", "wav", "flac"] "Unsupported audio file type, requires wav or flac."
    if ext in ["WAV", "wav"]
        signal, freq = WAV.wavread(file)
    else
        signal, freq = load(file)
    end
    @assert !isempty(signal[:, 1]) "$file seems to be empty, could it be corrupted?\nYou could delete it, or replace it with a known\ngood version from SD card or backup."
    return signal, freq
end

function resample_to_16000hz(signal, freq)
    signal = DSP.resample(signal, 16000.0f0 / freq; dims = 1)
    freq = 16000
    return signal, freq
end

############### PYTHON Opensoundscape ################
#=
# Dont forget conda activate opensoundscape
# Dont forget to modify file names and glob pattern
# Run script in Pomona-2, hard code trip date in the glob
# python /media/david/USB/Skraak/src/predict.py

from opensoundscape.torch.models.cnn import load_model
import opensoundscape

import torch
from pathlib import Path
import numpy as np
import pandas as pd

from glob import glob
import os
from datetime import datetime

model = load_model('/home/david/best.model')

# folders =  glob('./*/2023-?????/')
folders =  glob('./*/*/')
for folder in folders:
    os.chdir(folder)
    print(folder, ' start: ', datetime.now())
    # Beware, secretary island files are .wav
    field_recordings = glob('./*.[W,w][A,a][V,v]')
    scores, preds, unsafe = model.predict(
            field_recordings,
            binary_preds = 'single_target',
            overlap_fraction = 0.5,
            batch_size =  128,
            num_workers = 12)
    scores.to_csv("scores-2023-12-27.csv")
    preds.to_csv("preds-2023-12-27.csv")
    os.chdir('../..')
    print(folder, ' done: ', datetime.now())
    print()
    print()
=#
