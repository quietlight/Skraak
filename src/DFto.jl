# DFto.jl

module DFto

"""
DFto Submodules:
    audiodata_db

"""

export audiodata_db

using DataFrames, DBInterface, DuckDB, Random

"""
audiodata_db(df::DataFrame, table::String)

Takes a dataframe and inserts into AudioData.db table.
"""

function audiodata_db(df::DataFrame, table::String)
    temp_name = randstring(6)
    con = DBInterface.connect(DuckDB.DB, "/media/david/USB/AudioData.db")
    #con = DBInterface.connect(DuckDB.DB, "/Users/davidcary/Desktop/AudioData.db")
    DuckDB.register_data_frame(con, df, temp_name)
    DBInterface.execute(
        con,
        """
        INSERT
        INTO $table
        SELECT *
        FROM '$temp_name'
        """,
    )
    DBInterface.close!(con)
end

end #module
