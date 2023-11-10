# Skraak

Skraak can be useful to many conservation projects in NZ for identifying bird calls and monitoring call frequency.

__Skraak is intended to be simple to use for simple people like me.__

This package serves [skraak.kiwi](https://skraak.kiwi).

Most of the Skraak data has been recorded using Open Acoustics AudioMoth's or μMoth's at 16000 Hz. DOC recorders at 8000hz work fine.

Since Skraak works on colour png images, it can potentially be used for identifying predators in trail camera images too. Only 224px square, RGB, png's are required.

It is a good idea to use an Nvidia GPU. Everything should work fine on CPU, just slow. 

AMD GPU's are not supported but should be easy for you to get working. 

If you are doing serious work, start the julia repl with: julia -t n  where n is up to 1/2 the number of cores you have. I do 4, this is enough to keep up with a gamer GPU. 

__You can use Skraak too.__
```
[Install Julia](https://julialang.org/downloads/platform/), Julia-1.10 or newer
[git clone the project](https://github.com/quietlight/Skraak.git), if you dont have git you can download a zip file by clicking the <code> button

cd to your Skraak project
start the julia repl with $julia
type: ] (to enter Pkg mode)
type: activate .
type: instantiate
backspace to exit Pkg mode
exit repl with ctrl-D.

(You will want to install Revise and OhMyREPL, just do 'using Revise, OhMyREPL' in the Julia repl, add 'using Revise, OhMyREPL' to ~/.julia/config/startup.jl)
```

Later:
```
start the julia repl with $julia
type: ] (to enter Pkg mode)
type: dev path/to/Skraak (to make it a local package)
backspace to exit Pkg mode
type: using Skraak, Glob (glob is only here to help you refine your glob patterns)
```

1. Take some WAV's organised into a file structure LOCATION/TRIP_DATE/WAV_FILES 
2. and labels saved in a csv in the form:
    * file_path, label, "[start, end]" (where start and end are in seconds from the start of the wav file)
    * at least 2 label classes are required, for example Kiwi, NotKiwi
3. Generate a primary dataset of spectrogram images with the file structure:
    * DATASET/LABEL/PNG's (png files must be 224X224 px square, RGB). 
    * This structure is required, when training, the parent folder of a file is the label.
    * I use labels, [K, N] or [D, F, M, N] in words [Kiwi, Not] or [Duet, Female, Male, Not]. Anything will work
    * I have many thousands of K and N folders, the model does not care, as long as you remember folder containing png's is the label.

4. Train a Resnet18 model, either pretrained on Imagenet, or preferably the pretrained Skraak Kiwi model, which is currently trained on 7_400_000 images.
```
using Skraak
glob_pattern_1 = "Clips*/[D,F,M,N]/*.png" #for example. Note: requires png's as input.
glob_pattern_2 = "Dataset*/[K, N]/*.png"

# Train a model named Test1 for 2 epochs on png files found by glob_pattern, 
# start with a pretrained model.
train("Test1", 2, glob_pattern_1, true) 

# Train a model named Test2 for 2 epochs on png files found by glob_pattern, 
# train using model found at "path/to/model.jld2"
train("Test2", 2, glob_pattern_2, "path/to/model.jld2") 

# Note: Your unique text labels are sorted alphabetically, and converted to  
# integers, [1,2,3...] to be consumed by the flux model
# A text file will be saved beside the model.jld2, with the label to 
# integer mapping.
```
5. Run inference on raw data using a trained model
```
using Skraak
glob_pattern = "Clips/" #Note: requires folders as input. Folders contain flac, wav or png files.

# Predict classes of png, wav or flac files found in folders specified by 
# glob_pattern using model.jld2. A preds.csv file is saved in current directory
predict(glob_pattern, "path/to/model.jld2")
```
6. Generate audio clips and spectrogram images of all calls found.
```
make_clips("preds.csv")
```
7. Sort calls into subclasses (say: Duet, Female, Male, Something, Nothing) manually, or using a model combined with human supervision.

8. Store data from calls and file metadata in a DuckDB database for statistical analysis using SQL, DataFrames, Plots. 
```
I will not document this until the DuckDB api has stabilised. 
For now always store a csv backup using "EXPORT DATABASE 'Backup_2023-10-10';" in the duckdb cli.
I highly recommend storing data in a duckdb database. 
```
9. Repeat, iterating on your models as you accumulate more data. It's hard until it gets easy.

Managing datasets is like gardening, it takes some weeding and a _lot_ of compost (aka data) to get a good model growing. 

Julia is great for machine learning because it is realtively simple to get a GPU working. It does have disadvantages at GPT-4 scale, but for this kind of work it is excelent. Julia shines with any scientific computing task.
