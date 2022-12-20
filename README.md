# Skraak

```
using Glob, DataFrames, JSON3, DataFramesMeta, CSV

df = DataFrame(file = String[], start_time = Int64[], end_time = Int64[], absent_actual = Int64[], present_actual = Int64[])
wav_files = glob("*.WAV")
for f in wav_files
	for v in Iterators.countfrom(0, 5)
        v > 890 && break
        push!(df, [("/Volumes/Pomona-2/C05/2022-10-08/" * f), v, (v+5), 1, 0])
    end
end
@transform!(df, @byrow :key = (:file, :start_time, :end_time))

k_df = DataFrame(file = String[], start_time = Int64[], end_time = Int64[], absent_actual = Int64[], present_actual = Int64[])
data_files = glob("*.WAV.data")
for g in data_files
	json_string = read(g, String)
    dict = JSON3.read(json_string)
        if length(dict) > 1
        	for h in eachindex(dict[2:end])
        		st = dict[(h+1)][1]
                en = dict[(h+1)][2]
                for i in eachindex(dict[h+1][5])
                	if dict[(h+1)][5][i][:species] == "K-Set"
                		file_name = chop(g, tail=5)
                		start = 5*floor(st/5)
                		_end = 5*ceil(en/5)
                		#println((chop(g, tail=5)), "\t", 5*floor(st/5), "\t", 5*ceil(en/5))
                		for v in Iterators.countfrom(start, 5)
                        	v > _end-5 && break
                         	#println((chop(g, tail=5)), "\t", v, "\t", v+5, "\t0\t1")
                         	push!(k_df, [("/Volumes/Pomona-2/C05/2022-10-08/" * (chop(g, tail=5))), v, (v+5), 0, 1])
           				end
                	end
                end
        	end
        end	
end
@transform!(k_df, @byrow :key = (:file, :start_time, :end_time))

vect=k_df.key
new_df = filter(row -> !(row.key in vect), df)

joined_df=vcat(new_df, k_df, cols = :union)
sorted_df=sort(joined_df, [:file, :start_time])

a_df = select!(sorted_df, Not(:key))
CSV.write("actual_detections.csv", a_df)

ended up with 49 duplicates, easy csv found them, great, now it balances.

CSV.read("file.csv", DataFrame; kwargs)
actuals=CSV.read("actual_detections.csv", DataFrame)
preds=CSV.read("preds.csv", DataFrame)
 @transform!(actuals, @byrow :key = (:file, :start_time, :end_time))
 @transform!(preds, @byrow :key = (:file, :start_time, :end_time))
select!(actuals, Not([:file, :start_time, :end_time]))
select!(preds, Not([:file, :start_time, :end_time]))
aggregate=innerjoin(actuals, preds, on = :key)
```