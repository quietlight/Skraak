# Utility.jl

module Utility

"""
Utility submodules:
	UTCtoNZDT
	day_or_night

"""

export UTCtoNZDT, day_or_night

using Dates

"""
Takes a list of moth files and rewrites UTC filenames to NZDT, because since 
reconfiguring my moths at start of daylight saving they are recording UTC 
filenames which is not consistent with the way my notebook works.

a = glob("*/2022-12-17/")
for folder in a
    cd(folder)
    println(folder)
    files = glob("*.WAV")
	UTCtoNZDT(files)
    cd("/media/david/Pomona-2")
end

using Dates
"""

function UTCtoNZDT(files::Vector{String})
	fix_extension_of_files = []
	for old_file in files
		a = chop(old_file, tail = 4)
		d, t = split(a, "_")
		
		ye = parse(Int64, d[1:4])
		mo = parse(Int64, d[5:6])
		da = parse(Int64, d[7:8])
		ho = parse(Int64, t[1:2])
		mi = parse(Int64, t[3:4])
		se = parse(Int64, t[5:6])
		
		dt = DateTime(ye, mo, da, ho, mi, se)
		new_date = dt + Dates.Hour(13)
		# Must drop the WAV extension to avoiding force=true 
		# with  mv, since  the new file name may already exist and mv
		# will stacktrace leaving a big mess to tidy up.
		base_file = Dates.format(new_date, "yyyymmdd_HHMMSS") 
		temp_file = base_file * ".tmp"
		
		# Tuple to tidy extensions later
		tidy = (temp_file, base_file * ".WAV")

		mv(old_file, temp_file)
		push!(fix_extension_of_files, tidy)
		print(".")	
	end
	for item in fix_extension_of_files
			mv(item[1], item[2])
	end
	print("Tidy\n")
end

function day_or_night()
	return true
end

end # module

