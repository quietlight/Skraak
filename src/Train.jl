# Train.jl

export train #beware Flux.train! is not Skraak.train

import Base: length, getindex
import MLBase
using CUDA, Dates, Images, Flux, Glob, JLD2, Noise
using Random: shuffle!, seed!
using Metalhead: ResNet

#=
function train(
    model_name::String,
    train_epochs::Int64,
    images::Vector{String},
    pretrain::Model=true,
    train_test_split::Float64 = 0.8,
    batch_size::Int64 = 64,
)

Note:
Dont forget temp env,  julia -t 4
Assumes 224x224 pixel RGB images as png's
Saves jld2's in current directory

Use like:
using Skraak, Glob
images = glob("kiwi_set*/*/[N,K]/*.png") #11699814-element Vector{String}
model = "/media/david/SSD2/PrimaryDataset/model_K1-9_original_set_CPU_epoch-7-0.9924-2024-03-05.jld2"
train("K1-10_total_set_no_augumentation", 2, images, model, 0.97, 64)

images = glob("*/[D,F,M,N]/*.png") #from SSD2/Clips
model = "/media/david/SSD2/PrimaryDataset/model_K1-5_CPU_epoch-6-0.9795-2023-12-16.jld2"
train("DFMN1-5", 20, images, model)
=#
const LABELTOINDEX::Dict{String,Int32} = Dict()

Model = Union{Bool,String}

function train(
    model_name::String,
    train_epochs::Int64,
    images::Vector{String}, #glob_pattern::String = "*/*.png"
    pretrain::Model = true,
    train_test_split::Float64 = 0.8,
    batch_size::Int64 = 64,
)
    epochs = 1:train_epochs
    #images = glob(glob_pattern) #|> shuffle! |> x -> x[1:640]
    @assert !isempty(images) "No png images found"
    @info "$(length(images)) images in dataset"

    label_to_index = labels_to_dict(images)
    register_label_to_index!(label_to_index)
    @info "Text labels translate to: " label_to_index
    classes = length(label_to_index)
    @assert classes >= 2 "At least 2 label classes are required, for example: kiwi, not_kiwi"
    @info "$classes classes in dataset"
    @info "Device: $device"

    ceiling = seil(length(images), batch_size)
    train_test_index = train_test_idx(ceiling, batch_size, train_test_split)

    train, train_sample, test = process_data(images, train_test_index, ceiling, batch_size)
    @info "Made data loaders"

    model = load_model(pretrain, classes)
    @info "Loaded model"
    opt = Flux.setup(Flux.Optimisers.Adam(1e-5), model)
    @info "Setup optimiser"

    @info "Training for $epochs epochs: " now()
    training_loop!(
        model,
        opt,
        train,
        train_sample,
        test,
        epochs,
        model_name,
        classes,
        label_to_index
    )
    @info "Finished $(last(epochs)) epochs: " now()
end

struct ImageContainer{T<:Vector}
    img::T
end

struct ValidationImageContainer{T<:Vector}
    img::T
end

Container = Union{ImageContainer,ValidationImageContainer}

function seil(n::Int, batch_size::Int)
    return n ÷ batch_size * batch_size
end

function train_test_idx(ceiling::Int, batch_size::Int, train_test_split::Float64)::Int
    t =
        #! format: off
        ceiling ÷ batch_size * train_test_split |>
        round |>
        x -> x * batch_size |> 
        x -> convert(Int, x)
        #! format: on
end

function labels_to_dict(list::Vector{String})::Dict{String,Int32}
    l =
        #! format: off
        map(x -> split(x, "/")[end-1], list) |>
        unique |>
        sort |>
        x -> zip(x, 1:length(x)) |> 
        Dict
        #! format: on
    return l
end

"""
    register_label_to_index!(label_to_index::Dict{String,Int32})

    This will replace the content of the global variable LABELTOINDEX 
    with the content intended by the caller.

    Thanks algunion
    https://discourse.julialang.org/t/dataloader-scope-troubles/105207/4
"""
function register_label_to_index!(label_to_index::Dict{String,Int32})
    empty!(LABELTOINDEX)
    merge!(LABELTOINDEX, label_to_index)
end

device = CUDA.functional() ? gpu : cpu

function process_data(array_of_file_names, train_test_index, ceiling, batch_size)
    seed!(1234)
    images = shuffle!(array_of_file_names)
    train =
        ImageContainer(images[1:train_test_index]) |> x -> make_dataloader(x, batch_size)
    train_sample =
        ValidationImageContainer(images[1:(ceiling-train_test_index)]) |>
        x -> make_dataloader(x, batch_size)
    test =
        ValidationImageContainer(images[train_test_index+1:ceiling]) |>
        x -> make_dataloader(x, batch_size)
    return train, train_sample, test
end

length(data::ImageContainer) = length(data.img)
length(data::ValidationImageContainer) = length(data.img)

function getindex(data::ImageContainer{Vector{String}}, index::Int)
    path = data.img[index]
    img =
        #! format: off
        Images.load(path) |>
        #x -> Images.imresize(x, 224, 224) |>
        #x -> Images.RGB.(x) |>
        x -> Noise.add_gauss(x, (rand() * 0.2)) |>
        x -> apply_mask!(x, 3, 3, 12) |>
        x -> collect(channelview(float32.(x))) |> 
        x -> permutedims(x, (3, 2, 1))
        #! format: on
    y = LABELTOINDEX[(split(path, "/")[end-1])]
    return img, y
end

function getindex(data::ValidationImageContainer{Vector{String}}, index::Int)
    path = data.img[index]
    img =
        #! format: off
        Images.load(path) |>
        #x -> Images.imresize(x, 224, 224) |>
        #x -> Images.RGB.(x) |>
        x -> collect(channelview(float32.(x))) |> 
        x -> permutedims(x, (3, 2, 1))
        #! format: on
    y = LABELTOINDEX[(split(path, "/")[end-1])]
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

# see load_model() from predict, and below
function load_model(pretrain::Bool, classes::Int64)
    fst = Metalhead.ResNet(18, pretrain = pretrain).layers
    lst = Flux.Chain(AdaptiveMeanPool((1, 1)), Flux.flatten, Dense(512 => classes))
    model = Flux.Chain(fst[1], lst) |> device
    return model
end

#If model classes == desired classes I don't empty the last layer
#That means that I can just train from where I left off for new data, DFMN model
#Could be a gotcha if I want to train a different 4 class model, no need for a switch just yet
function load_model(model_path::String, classes::Int64)
    model_state = JLD2.load(model_path, "model_state")
    model_classes = length(model_state[1][2][1][3][2])
    f = Metalhead.ResNet(18, pretrain = false).layers
    l = Flux.Chain(AdaptiveMeanPool((1, 1)), Flux.flatten, Dense(512 => model_classes))
    m = Flux.Chain(f[1], l)
    Flux.loadmodel!(m, model_state)
    if classes == model_classes
        model = m |> device
    else
        fst = m.layers
        lst = Flux.Chain(AdaptiveMeanPool((1, 1)), Flux.flatten, Dense(512 => classes))
        model = Flux.Chain(fst[1], lst) |> device
    end
    return model
end

function evaluate(m, d, c)
    good = 0
    count = 0
    pred = Int64[]
    actual = Int64[]
    for (x, y) in d
        p = Flux.onecold(m(x))
        good += sum(p .== y)
        count += length(y)
        append!(pred, p)
        append!(actual, y)
    end
    accuracy = round(good / count, digits = 4)
    confusion_matrix =
        MLBase.confusmat(c, actual, pred)
        #freqtable(DataFrame(targets = actual, predicts = pred), :targets, :predicts)
    #roc=MLBase.roc(actual, pred, 100)
    #f1=MLBase.f1score(roc)
    return accuracy, confusion_matrix #, roc, f1
end

function train_epoch!(model; opt, train, classes)
    Flux.train!(model, train, opt) do m, x, y
        Flux.Losses.logitcrossentropy(m(x), Flux.onehotbatch(y, 1:classes))
    end
end

function dict_to_text_file(dict, model_name)
    text = ""
    for (key, value) in dict
        text = text * "$(key) => $(value)\n"
    end
    open("labels_$(model_name)-$(today()).txt", "w") do file
        write(file, text)
    end
    @info "Saved labels to file for future reference"
end

function training_loop!(
    model,
    opt,
    train,
    train_sample,
    test,
    epochs::UnitRange{Int64},
    model_name::String,
    classes,
    label_to_index,
)
    @time eval, vcm = evaluate(model, test, classes)
    @info "warm up accuracy" accuracy = eval
    @info "warm up confusion matrix" vcm

    a = 0
    for epoch in epochs
        println("")
        @info "Starting Epoch: $epoch"
        epoch == 1 && dict_to_text_file(label_to_index, model_name)
        @time train_epoch!(model; opt, train, classes)
        @time train_accuracy, train_confusion_matrix = evaluate(model, train_sample, classes)
        @info "Epoch: $epoch"
        @info "train" accuracy = train_accuracy
        @info "train" train_confusion_matrix

        @time test_accuracy, test_confusion_matrix = evaluate(model, test, classes)
        @info "test" accuracy = test_accuracy
        @info "test" test_confusion_matrix

        # number kiwi guessed right, assumes kiwi=1, not=2 (alphabetical)
        #test_confusion_matrix[1,1] > a && begin
            #a = test_confusion_matrix[1,1]
            let _model = cpu(model)
                jldsave(
                    "model_$(model_name)_CPU_epoch-$epoch-$test_accuracy-$(today()).jld2";
                    model_state = Flux.state(_model),
                )
                @info "Saved a best_model"
            end
        #end
    end
end
