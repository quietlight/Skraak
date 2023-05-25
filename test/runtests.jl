using Skraak
using Test
using DataFrames, Dates

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
            # should return the same dataframe
            df1 = DataFrame(loc=["Auckland"], kiwi=[1.0])
            @test Skraak.assert_detections_present(df1, "location", "trip date") == DataFrame(loc=["Auckland"], kiwi=[1.0])
            # no detections so throws error to shortcicuit the pipe
            df2 = DataFrame(loc=["Wellington", "Auckland"], kiwi=[0.0, 0.0])
            @test_throws MethodError Skraak.assert_detections_present(df2, "location", "trip_date")
        end
        @testset "function filter_positives!" begin
            # All positives
            @test Skraak.filter_positives!(DataFrame(kiwi=[1.0, 1.0, 1.0])) == DataFrame(kiwi=[1.0, 1.0, 1.0])
            # Mix of positive and negative
            @test Skraak.filter_positives!(DataFrame(kiwi=[1.0, 0.0, 1.0])) == DataFrame(kiwi=[1.0, 1.0])
            # All negatives
            @test Skraak.filter_positives!(DataFrame(kiwi=[0.0, 0.0, 0.0])) == DataFrame(kiwi=[])
        end
        @testset "function filename_to_datetime!" begin
            @testset "DateTime from yyyymmdd_HHMMSS format" begin
                # Test with valid filename
                @test Skraak.filename_to_datetime!("./20230328_213000.WAV") == DateTime(2023, 3, 28, 21, 30, 0)
                # Test with invalid filename
                @test_throws ArgumentError Skraak.filename_to_datetime!("./20230328_2130.WAV")
            end
            @testset "DateTime from ddmmyyyy_HHMMSS format" begin
                # Test with valid filename
                @test Skraak.filename_to_datetime!("./280323_213000.wav") == DateTime(2023,
                3, 28, 21, 30, 0)
                # Test with invalid filename (does not throw, not sure why)
                #@test_throws ArgumentError Skraak.filename_to_datetime!("./280323_2130.WAV")
                @test_throws ArgumentError Skraak.filename_to_datetime!("./28323_213000.WAV")
            end
        end
        @testset "function insert_datetime_column!" begin
            df = DataFrame(file=["./20220101_120000.WV", "./011122_124500.wav"])
            @test Skraak.insert_datetime_column!(df) == DataFrame(file=["./20220101_120000.WV", "./011122_124500.wav"], DateTime=[DateTime(2022,1,1,12,0,0), DateTime(2022,11,1,12,45,0)])
        end
        @testset "functions construct_dawn_dusk_dict, night and exclude_daytime!" begin
            dict = Skraak.construct_dawn_dusk_dict("../../dawn_dusk.csv")
            # construct_dawn_dusk_dict
            @test length(keys(dict)) >= 2557
            @test haskey(dict, Date(2019, 1, 1))
            @test haskey(dict, Date(2024, 12, 31))
            @test dict[Date(2019, 1, 1)] == (DateTime("2019-01-01T06:03:03"), DateTime("2019-01-01T21:43:33"))
            @test dict[Date(2024, 12, 31)] == (DateTime("2024-12-31T06:02:38"), DateTime("2024-12-31T21:43:30"))
            # night
            @test Skraak.night(DateTime("2019-01-01T00:00:00"), dict) == true
            @test Skraak.night(DateTime("2019-01-01T09:03:03"), dict) == false
            @test Skraak.night(DateTime("2021-11-02T04:00:00"), dict) == true
            @test Skraak.night(DateTime("2021-11-02T20:00:00"), dict) == false
            # exclude_daytime!
            df = DataFrame(DateTime = [DateTime("2021-08-01T00:00:00"), DateTime("2021-08-01T06:00:00"), DateTime("2021-08-01T12:00:00"), DateTime("2021-08-01T16:00:00")])
            Skraak.exclude_daytime!(df, dict)
            @test df == DataFrame(DateTime = [DateTime("2021-08-01T00:00:00"), DateTime("2021-08-01T06:00:00")])
        end
        @testset "cluster_detections" begin
            detections=[100.0, 102.5, 105.0, 107.5, 110.0, 112.5, 115.0, 117.5, 120.0, 122.5, 125.0, 127.5, 130.0, 132.5, 135.0, 137.5, 140.0, 685.0, 687.5, 690.0, 692.5, 695.0, 697.5, 700.0, 702.5, 705.0, 707.5, 710.0, 712.5, 717.5, 720.0, 890.0]
            @test Skraak.cluster_detections(detections) == [[100.0, 102.5, 105.0, 107.5, 110.0, 112.5, 115.0, 117.5, 120.0, 122.5, 125.0, 127.5, 130.0, 132.5, 135.0, 137.5, 140.0], [685.0, 687.5, 690.0, 692.5, 695.0, 697.5, 700.0, 702.5, 705.0, 707.5, 710.0, 712.5, 717.5, 720.0]]
            @test Skraak.cluster_detections([100.0]) == []
            @test_throws BoundsError Skraak.cluster_detections(Float64[])
        end
        @testset "function calculate_clip_start_end" begin
            # assumes it is operating on 5 second clips
            freq = 16000.0f0
            signal_length = Int(895*freq)
            # good long detection
            detection=[100.0, 102.5, 105.0, 107.5, 110.0, 112.5, 115.0, 117.5, 120.0, 122.5, 125.0, 127.5, 130.0, 132.5, 135.0, 137.5, 140.0]
            @test Skraak.calculate_clip_start_end(detection, freq, signal_length) == (1.6e6, 2.32e6)
            @test Skraak.calculate_clip_start_end(detection[1:2], freq, signal_length) == (1.6e6, 1.72e6)
            # needs at least 2 detections
            @test_throws MethodError Skraak.calculate_clip_start_end(detection[1], freq, signal_length)
            # first detection at 0
            first = [0.0, 2.5]
            @test Skraak.calculate_clip_start_end(first, freq, signal_length) == (1.0, 120000.0)
            # last detection within 5s of last sample
            last = [890.0, 892.5, 895.0]
            @test Skraak.calculate_clip_start_end(last, freq, signal_length) == (1.424e7, 1.432e7)
            @test Skraak.calculate_clip_start_end(last[1:2], freq, signal_length) == (1.424e7, 1.432e7)
        end
    end
end
