# CSVto.jl

module CSVto

"""
CSVto submodules:
dataset

# cd to the working directory, to run:
julia
using Glob, Skraak
x = glob("*/2022-10-08")
for i in x
cd(i)
Skraak.CSVto.dataset()
cd("/path/to/working/directory")
end
"""

export dataset

using Glob, CSV, DataFrames, DelimitedFiles

"""
dataset()

This function takes a csv file in a folder plus
a label string, and generates a dataset.
It saves  wav files to /media/david/72CADE2ECADDEDFB/DataSet/$label/  
and annotation files to a reperate directory as raven files. (it would be 
better for me to just build the csv direct, soon.)

using Glob, CSV, DataFrames, DelimitedFiles
"""

function dataset(label::String)
    raw_data = glob("*.csv")

    if length(raw_data) == 1
        data_frame = DataFrame(CSV.File(raw_data[1]))
    else
        message = pwd() * " has no csv present, or more than 1"
        return message
    end

    if !(label in levels(data_frame.species))
        message = pwd() * " $label not present"
        return message
    else
        species_grouped_frames = groupby(data_frame, :species)
        # Beware this comma!!
        kset = species_grouped_frames[(species = :"$label",)]
        path_sets = groupby(kset, :Path)

        for file_path in eachindex(path_sets)
            f = path_sets[file_path]
            data = Any[]
            headers = Any[
                "Selection",
                "View",
                "Channel",
                "start_time",
                "end_time",
                "low_f",
                "high_f",
                "Species",
                "Notes",
            ]
            push!(data, headers)
            for (index, row) in enumerate(eachrow(f))
                push!(
                    data,
                    [
                        index,
                        "Spectrogram 1",
                        "1",
                        row."Start Time (relative)",
                        row."End Time (relative)",
                        row.min_freq,
                        row.max_freq,
                        row.species,
                        "",
                    ],
                )
            end

            p = split(f[1, :Path], ".")
            if length(p) < 3
                # Julia does not like | in file names, but all my csv files as 
                # already built with | in file path.
                # But I need q[1] later when I save the wav anyway, so its ok
                q = split(p[end-1], "|")
                r = string(q[1], "_", q[2], "_", q[3])
                output_file =
                    "/media/david/72CADE2ECADDEDFB/DataSet/$label-Annotations/" *
                    r *
                    ".Table.1.selections.txt"
            else
                error("File names have gone to hell. One period only David")
            end
            open(output_file, "w") do io
                writedlm(io, data, '\t')
            end
            src = chop(f[1, :File_Name], tail = 5)
            dst =
                "/media/david/72CADE2ECADDEDFB/DataSet/$label/" *
                q[1] *
                "_" *
                q[2] *
                "_" *
                src
            cp(src, dst, force = true)
            print(".")
        end
    end

end

end  # module
