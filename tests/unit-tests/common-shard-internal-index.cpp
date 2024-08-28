#include "zarr.common.hh"
#include "logger.hh"

#include <stdexcept>

#define EXPECT_EQ(a, b)                                                        \
    EXPECT((a) == (b), "Expected %s == %s, but %zu != %zu", #a, #b, a, b)

int
main()
{
    int retval = 1;

    std::vector<zarr::Dimension> dims;
    dims.emplace_back("t",
                      ZarrDimensionType_Time,
                      0,
                      32, // 32 timepoints / chunk
                      1); // 1 shard
    dims.emplace_back("y",
                      ZarrDimensionType_Space,
                      960,
                      320, // 3 chunks
                      2);  // 2 ragged shards
    dims.emplace_back("x",
                      ZarrDimensionType_Space,
                      1080,
                      270, // 4 chunks
                      3);  // 2 ragged shards    

    try {
        EXPECT_EQ(zarr::shard_index_for_chunk(0, dims), 0);
        EXPECT_EQ(zarr::shard_internal_index(0, dims), 0);

        EXPECT_EQ(zarr::shard_index_for_chunk(1, dims), 0);
        EXPECT_EQ(zarr::shard_internal_index(1, dims), 1);

        EXPECT_EQ(zarr::shard_index_for_chunk(2, dims), 0);
        EXPECT_EQ(zarr::shard_internal_index(2, dims), 2);

        EXPECT_EQ(zarr::shard_index_for_chunk(3, dims), 1);
        EXPECT_EQ(zarr::shard_internal_index(3, dims), 0);

        EXPECT_EQ(zarr::shard_index_for_chunk(4, dims), 0);
        EXPECT_EQ(zarr::shard_internal_index(4, dims), 3);

        EXPECT_EQ(zarr::shard_index_for_chunk(5, dims), 0);
        EXPECT_EQ(zarr::shard_internal_index(5, dims), 4);

        EXPECT_EQ(zarr::shard_index_for_chunk(6, dims), 0);
        EXPECT_EQ(zarr::shard_internal_index(6, dims), 5);

        EXPECT_EQ(zarr::shard_index_for_chunk(7, dims), 1);
        EXPECT_EQ(zarr::shard_internal_index(7, dims), 3);

        EXPECT_EQ(zarr::shard_index_for_chunk(8, dims), 2);
        EXPECT_EQ(zarr::shard_internal_index(8, dims), 0);

        EXPECT_EQ(zarr::shard_index_for_chunk(9, dims), 2);
        EXPECT_EQ(zarr::shard_internal_index(9, dims), 1);

        EXPECT_EQ(zarr::shard_index_for_chunk(10, dims), 2);
        EXPECT_EQ(zarr::shard_internal_index(10, dims), 2);

        EXPECT_EQ(zarr::shard_index_for_chunk(11, dims), 3);
        EXPECT_EQ(zarr::shard_internal_index(11, dims), 0);
        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: %s\n", exc.what());
    } catch (...) {
        LOG_ERROR("Exception: (unknown)");
    }

    return retval;
}