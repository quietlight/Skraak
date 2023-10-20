# Train.jl

import Base: length, getindex
using CUDA, DataFrames, Dates, Images, Flux, FreqTables, Glob, JLD2, Metalhead, Noise
using Random: shuffle!, seed!

export train #beware Flux.train! is not Skraak.train

#=
function train(
        glob_pattern::String, 
        label_to_index::Dict{String,Int32}, #may have to be in global scope too
        pretrain::Bool,
        epochs::UnitRange{Int64},
        model_name::String,
        train_test_split::Float64 = 0.8,
        batch_size::Int64 = 64
        )

Note:
Dont forget temp env,  julia -t 4
Assumes 224x224 pixel RGB images as png's
Saves jld2's in current directory

use like:
glob_pattern = "Clips_2023-09-11/[D,F,M,N]/*.png" #from SSD1
label_to_index = Dict{String,Int32}("D" => 1, "F" => 2, "M" => 3, "N" => 4)
train(glob_pattern, label_to_index, true, 1:2, "Test")
train(glob_pattern, label_to_index, "/media/david/SSD2/model_K1-3_CPU_epoch-10-0.9965-2023-10-18T17:32:36.747.jld2", 1:2, "Test2")

=#
Model = Union{Bool,String}

function train(
    glob_pattern::String,
    label_to_index::Dict{String,Int32}, #may have to be in global scope too
    pretrain::Model,
    epochs::UnitRange{Int64},
    model_name::String,
    train_test_split::Float64 = 0.8,
    batch_size::Int64 = 64,
)
    imgs = glob(glob_pattern) |> shuffle! |> x -> x[1:500]
    @info "$(length(imgs)) images in dataset"

    ceiling = length(imgs) ÷ batch_size * batch_size
    train_test_index =
        ceiling ÷ batch_size * train_test_split |>
        round |>
        x -> x * batch_size |> x -> convert(Int, x)
    classes = length(label_to_index)
    @info "$classes classes in dataset"
    @info "Device: $device"
    train, train_sample, test = process_data(imgs, train_test_index, ceiling, batch_size)
    @info "Made data loaders"
    model = load_model(pretrain, classes)
    @info "Loaded model"
    opt = Flux.setup(Flux.Optimisers.Adam(1e-5), model)
    @info "Setup optimiser"
    @info "Training for $epochs epochs: " now()
    training_loop!(model, opt, train, train_sample, test, epochs, model_name, classes)
    @info "Finished $(last(epochs)) epochs: " now()
end

device = CUDA.functional() ? gpu : cpu

struct ImageContainer{T<:Vector}
    img::T
end

struct ValidationImageContainer{T<:Vector}
    img::T
end

Container = Union{ImageContainer,ValidationImageContainer}

function process_data(array_of_file_names, train_test_index, ceiling, batch_size)
    seed!(1234)
    imgs = shuffle!(array_of_file_names)
    train = ImageContainer(imgs[1:train_test_index]) |> x -> make_dataloader(x, batch_size)
    train_sample =
        ValidationImageContainer(imgs[1:(ceiling-train_test_index)]) |>
        x -> make_dataloader(x, batch_size)
    test =
        ValidationImageContainer(imgs[train_test_index+1:ceiling]) |>
        x -> make_dataloader(x, batch_size)
    return train, train_sample, test
end

length(data::ImageContainer) = length(data.img)
length(data::ValidationImageContainer) = length(data.img)

function getindex(data::ImageContainer{Vector{String}}, idx::Int)
    path = data.img[idx]
    img =
        Images.load(path) |>
        #x -> Images.imresize(x, 224, 224) |>
        x ->
            Noise.add_gauss(x, (rand() * 0.2)) |>
            x ->
                apply_mask!(x, 3, 3, 12) |>
                x -> collect(channelview(float32.(x))) |> x -> permutedims(x, (3, 2, 1))

    label_to_index = Dict{String,Int32}("D" => 1, "F" => 2, "M" => 3, "N" => 4)
    y = label_to_index[(split(path, "/")[end-1])] #not sure this is in scope
    return img, y
end

function getindex(data::ValidationImageContainer{Vector{String}}, idx::Int)
    path = data.img[idx]
    img =
        Images.load(path) |>
        #x -> Images.imresize(x, 224, 224) |>
        x -> collect(channelview(float32.(x))) |> x -> permutedims(x, (3, 2, 1))

    label_to_index = Dict{String,Int32}("D" => 1, "F" => 2, "M" => 3, "N" => 4)
    y = label_to_index[(split(path, "/")[end-1])] #not sure this is in scope
    return img, y
end

# assumes 224px square images
function apply_mask!(
    img::Array{RGB{N0f8},2},
    max_number::Int = 3,
    min_size::Int = 3,
    max_size::Int = 22,
)
    # horizontal
    for range in get_random_ranges(max_number, min_size, max_size)
        img[range, :] .= RGB{N0f8}(0.7, 0.7, 0.7)
    end
    # vertical
    for range in get_random_ranges(max_number, min_size, max_size)
        img[:, range] .= RGB{N0f8}(0.7, 0.7, 0.7)
    end
    return img
end

# assumes 224px square images
function get_random_ranges(max_number::Int, min_size::Int, max_size::Int)
    number = rand(0:max_number)
    ranges = []
    while length(ranges) < number
        start = rand(1:224)
        size = rand(min_size:max_size)
        if start + size > 224
            continue
        end
        push!(ranges, start:start+size)
    end
    return ranges
end

function make_dataloader(container::Container, batch_size::Int)
    data =
        Flux.DataLoader(container; batchsize = batch_size, collate = true, parallel = true)
    device == gpu ? data = CuIterator(data) : nothing
    return data
end

function load_model(pretrain::Bool, classes::Int64)
    fst = Metalhead.ResNet(18, pretrain = pretrain).layers
    lst = Flux.Chain(AdaptiveMeanPool((1, 1)), Flux.flatten, Dense(512 => classes))
    model = Flux.Chain(fst[1], lst) |> device
    return model
end

function load_model(model_path::String, classes::Int64)
    model_state = JLD2.load(model_path, "model_state")
    model_classes = length(model_state[1][2][1][3][2])
    f = Metalhead.ResNet(18, pretrain = false).layers
    l = Flux.Chain(AdaptiveMeanPool((1, 1)), Flux.flatten, Dense(512 => model_classes))
    m = Flux.Chain(f[1], l)
    Flux.loadmodel!(m, model_state)
    fst = m.layers
    lst = Flux.Chain(AdaptiveMeanPool((1, 1)), Flux.flatten, Dense(512 => classes))
    model = Flux.Chain(fst[1], lst) |> device
    return model
end

function evaluate(m, d)
    good = 0
    count = 0
    pred = []
    actual = []
    for (x, y) in d
        p = Flux.onecold(m(x))
        good += sum(p .== y)
        count += length(y)
        append!(pred, p)
        append!(actual, y)
    end
    accuracy = round(good / count, digits = 4)
    confusion_matrix =
        freqtable(DataFrame(targets = actual, predicts = pred), :targets, :predicts)
    return accuracy, confusion_matrix
end

function train_epoch!(model; opt, train, classes)
    Flux.train!(model, train, opt) do m, x, y
        Flux.Losses.logitcrossentropy(m(x), Flux.onehotbatch(y, 1:classes))
    end
end

function training_loop!(
    model,
    opt,
    train,
    train_sample,
    test,
    epochs::UnitRange{Int64},
    model_name,
    classes,
)
    @time eval, vcm = evaluate(model, test)
    @info "warm up" accuracy = eval
    @info "warm up" vcm

    a = 0.0
    for epoch in epochs
        println("")
        @info "Starting Epoch: $epoch"
        @time train_epoch!(model; opt, train, classes)
        @time metric_train, train_confusion_matrix = evaluate(model, train_sample)
        @info "Epoch: $epoch"
        @info "train" accuracy = metric_train
        @info "train" train_confusion_matrix

        @time metric_test, test_confusion_matrix = evaluate(model, test)
        @info "test" accuracy = metric_test
        @info "test" test_confusion_matrix

        metric_test > a && begin
            a = metric_test
            let _model = cpu(model)
                jldsave(
                    "model_$(model_name)_CPU_epoch-$epoch-$metric_test-$(now()).jld2";
                    model_state = Flux.state(_model),
                )
                @info "Saved a best_model"
            end
        end
    end
end
