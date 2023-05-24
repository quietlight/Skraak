using Skraak
using Test
using DataFrames

@testset "Skraak.jl" begin
    @testset "make_clips functon" begin
        @testset "function Skraak.assert_not_empty" begin
            # Empty dataframe should throw an error
            df1 = DataFrame()
            @test_throws MethodError Skraak.assert_not_empty(df1, "/abc/def")
            # Non-empty dataframe should return the same dataframe
            df2 = DataFrame(A=[1,2,3], B=[4,5,6])
            @test Skraak.assert_not_empty(df2, "/abc/def") == DataFrame(A=[1,2,3], B=[4,5,6])
        end
        @testset "function Skraak.rename_column!" begin
            # Should do nothing if col does not exist
            df1 = DataFrame(A = [1, 2], B = [3, 4])
            @test Skraak.rename_column!(df1, "C", "D") == DataFrame(A = [1, 2], B = [3, 4])
            # existing column should be renamed
            df2 = DataFrame(A = [1, 2], B = [1, 2])
            @test Skraak.rename_column!(df2, "B", "C") == DataFrame(A = [1, 2], C = [1, 2])
        end
        @testset "function Skraak.assert_detections_present" begin
            # should return the same dataframe or throw
            df1 = DataFrame(loc=["Auckland"], kiwi=[1.0])
            @test Skraak.assert_detections_present(df1, "location", "trip date") == DataFrame(loc=["Auckland"], kiwi=[1.0])
            # no detections so throws error to shortcicuit the pipe
            df2 = DataFrame(loc=["Wellington", "Auckland"], kiwi=[0.0, 0.0])
            @test_throws MethodError Skraak.assert_detections_present(df2, "location", "trip_date")
        end
    end


end
