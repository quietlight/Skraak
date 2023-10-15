# Train.jl

using Random: shuffle!
using Random: seed!
import Base: length
import Base: getindex
using CUDA, DataFrames, Dates, Images, Flux, FreqTables, Glob, JLD2, Metalhead, Noise

export train!

```
Note:
Dont forget temp env,  julia -t 4
Assumes 224x224 pixel RGB images as png's

Saves jld2's in current directory
```
#=
use like:
glob_pattern = "2023-09-*/*/*/[N,K]/*.png" #from SSD2
label_to_index = Dict{String,Int32}("K" => 1, "N" => 2)
train!(glob_pattern, label_to_index, true, 1:9, "K1-3")

#using BSON: @save
#BSON.@save "model_K1-3_CPU_epoch-$epoch-$metric_test-$(now()).bson" _model
=#
function train!(
        glob_pattern::String, 
        label_to_index::Dict{String,Int32}, #may have to be in global scope too
        pretrain::Bool,
        epochs::UnitRange{Int64},
        model_name::String,
        train_test_split::Float64 = 0.95,
        batch_size::Int64 = 64
        )
    ceiling = length(data) ÷ batch_size * batch_size
    train_test_index = ceiling ÷ batch_size * train_test_split |> 
        round |> 
        x -> x * batch_size |> 
        x -> convert(Int, x)
    classes = length(label_to_index)

    imgs = glob(glob_pattern) |>
        x -> process_data(x)
    @info imgs

    train, train_sample, test = process_data(imgs, train_test_index, ceiling)
    model = load_model(pretrain, classes)
    opt = Flux.setup(Flux.Optimisers.Adam(1e-5), model)
    @info "Training for $(last(epochs)) epochs, start: " now()
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

function process_data(array_of_file_names, train_test_index, ceiling)
    seed!(1234);
    imgs = shuffle!(array_of_file_names) |>
        x -> ImageContainer(x)
    train = make_dataloader(ImageContainer(imgs[1:train_test_index]), batch_size)
    train_sample = make_dataloader(ValidationImageContainer(imgs[1:(ceiling-train_test_index)]), batch_size)
    test = make_dataloader(ValidationImageContainer(imgs[train_test_index+1:ceiling]), batch_size)
    return train, train_sample, test
end

length(data::ImageContainer) = length(data.img)
length(data::ValidationImageContainer) = length(data.img)

function getindex(data::ImageContainer{Vector{String}}, idx::Int)
    path = data.img[idx]
    img = Images.load(path) |>
        #x -> Images.imresize(x, 224, 224) |>
        x -> Noise.add_gauss(x, (rand() * 0.2)) |>
        x -> apply_mask!(x, 3, 3, 12) |>
        x -> collect(channelview(float32.(x))) |> 
        x -> permutedims(x, (3, 2, 1))
    y = label_to_index[(split(path, "/")[end-1])] #not sure this is in scope
    return img, y
end

function getindex(data::ValidationImageContainer{Vector{String}}, idx::Int)
    path = data.img[idx]
    img =
        Images.load(path) |>
        #x -> Images.imresize(x, 224, 224) |>
        x -> collect(channelview(float32.(x))) |> 
        x -> permutedims(x, (3, 2, 1))
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
    data = Flux.DataLoader(
    container;
    batchsize = batch_size,
    collate = true,
    parallel = true,
    )
    device == gpu ? data = CuIterator(data) : nothing
    return data
end

function load_model(pretrain::Bool, classes::Int64)
    fst = Metalhead.ResNet(18, pretrain = pretrain).layers
    lst = Flux.Chain(AdaptiveMeanPool((1, 1)), Flux.flatten, Dense(512 => classes));
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

function training_loop!(model, opt, train, train_sample, test, epochs::UnitRange{Int64}, model_name, classes)
    @time eval, vcm = evaluate(model, test)
    @info "warm up" accuracy = eval
    @info "warm up" vcm

    a = 0.0
    for epoch in epochs
        println("")
        @info "Starting Epoch: $epoch"
        @time train_epoch!(model; opt, train, classes)
        @time metric_train, train_confusion_matrix = evaluate(model, train_sample)
        @info "Epoch: " epoch
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

