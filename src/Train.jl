# Train.jl

# https://github.com/FluxML/model-zoo/blob/master/tutorials/transfer_learning/transfer_learning.jl
# This works on my data IT TRAINS best, but only -t 4
# dont forget temp env

using Random: shuffle!
using Random: seed!
import Base: length
import Base: getindex
using Images
using Flux
using CUDA
using Metalhead
using Noise
using Glob
using BSON: @save
using Dates
#using CSV
using DataFrames
using FreqTables
using JLD2
using Logging, LoggingExtras

imgs = glob("2023-09-*/*/*/[N,K]/*.png") #from SSD2

seed!(1234);
shuffle!(imgs)

#CSV.write("files.csv", DataFrame(file=imgs))

device = CUDA.functional() ? gpu : cpu

struct ImageContainer{T<:Vector}
    img::T
end

struct ValidationImageContainer{T<:Vector}
    img::T
end

data = ImageContainer(imgs)
#val_data = ValidationImageContainer(imgs)

length(data::ImageContainer) = length(data.img)
length(data::ValidationImageContainer) = length(data.img)

const im_size = (224, 224)
name_to_idx = Dict{String,Int32}("K" => 1, "N" => 2)

function getindex(data::ImageContainer{Vector{String}}, idx::Int)
    path = data.img[idx]
    img =
        Images.load(path) |>
        x ->
            Images.imresize(x, 224, 224) |>
            x ->
                Noise.add_gauss(x, (rand() * 0.2)) |>
                x ->
                    apply_mask(x, 3, 3, 12) |>
                    x -> collect(channelview(float32.(x))) |> x -> permutedims(x, (3, 2, 1))
    y = name_to_idx[(split(path, "/")[end-1])]
    return img, y
end

function getindex(data::ValidationImageContainer{Vector{String}}, idx::Int)
    path = data.img[idx]
    img =
        Images.load(path) |>
        x ->
            Images.imresize(x, 224, 224) |>
            x -> collect(channelview(float32.(x))) |> x -> permutedims(x, (3, 2, 1))
    y = name_to_idx[(split(path, "/")[end-1])]
    return img, y
end

# assumes 224px square images
function apply_mask(
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

# define DataLoaders
const batch_size = 64
const train_test_split = 0.95
const ceiling = length(data) ÷ batch_size * batch_size
const train_test_index =
    ceiling ÷ batch_size * train_test_split |> round |> x -> x * batch_size |> Int

train = Flux.DataLoader(
    ImageContainer(imgs[1:train_test_index]);
    batchsize = batch_size,
    collate = true,
    parallel = true,
)
device == gpu ? train = CuIterator(train) : nothing

train_sample = Flux.DataLoader(
    ValidationImageContainer(imgs[1:(ceiling-train_test_index)]);
    batchsize = batch_size,
    collate = true,
    parallel = true,
)
device == gpu ? train_sample = CuIterator(train_sample) : nothing

test = Flux.DataLoader(
    ValidationImageContainer(imgs[train_test_index+1:ceiling]);
    batchsize = batch_size,
    collate = true,
    parallel = true,
)
device == gpu ? test = CuIterator(test) : nothing

fst = Metalhead.ResNet(18, pretrain = true).layers
# BEWARE NUMBER CLASSES
lst = Flux.Chain(AdaptiveMeanPool((1, 1)), Flux.flatten, Dense(512 => 2));
model = Flux.Chain(fst[1], lst) |> device

function eval_f(m, d)
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

# BEWARE NUMBER CLASSES
function train_epoch!(model; opt, train)
    Flux.train!(model, train, opt) do m, x, y
        Flux.Losses.logitcrossentropy(m(x), Flux.onehotbatch(y, 1:2))
    end
end

opt = Flux.setup(Flux.Optimisers.Adam(1e-5), model);

logger = FileLogger("logfile.txt"; append = true)

@time metric_eval, v_confusion_matrix = eval_f(model, test)
with_logger(logger) do
    @info "eval" accuracy = metric_eval
    @info "eval" v_confusion_matrix
end

a = 0.0
for iter in 1:9
    println("")
    println("Epoch: $iter")
    @time train_epoch!(model; opt, train)
    @time metric_train, t_confusion_matrix = eval_f(model, train_sample)
    with_logger(logger) do
        @info "Epoch: " iter
        @info "train" accuracy = metric_train
        @info "train" t_confusion_matrix
    end
    @time metric_eval, v_confusion_matrix = eval_f(model, test)
    with_logger(logger) do
        @info "test" accuracy = metric_eval
        @info "test" v_confusion_matrix
    end
    metric_eval > a && begin
        a = metric_eval
        let _model = cpu(model)
            jldsave(
                "model_K1-3_CPU_epoch-$iter-$metric_eval-$(now()).jld2";
                model_state = Flux.state(_model),
            )
            #BSON.@save "model_K1-3_CPU_epoch-$iter-$metric_eval-$(now()).bson" _model
            with_logger(logger) do
                @info "Saved a best_model"
            end
        end
    end
end