#include "zarr.common.hh"
#include "logger.hh"

#include <stdexcept>

#define EXPECT_EQ(a, b)                                                        \
    EXPECT((a) == (b), "Expected %s == %s, but %zu != %zu", #a, #b, a, b)

int main()
{
    int retval = 1;

    std::vector<zarr::Dimension> dims;
    dims.emplace_back(
      "t", ZarrDimensionType_Time, 0, 5, 0); // 5 timepoints / chunk
    dims.emplace_back("c", ZarrDimensionType_Channel, 3, 2, 0); // 2 chunks
    dims.emplace_back("z", ZarrDimensionType_Space, 5, 2, 0);   // 3 chunks
    dims.emplace_back("y", ZarrDimensionType_Space, 48, 16, 0); // 3 chunks
    dims.emplace_back("x", ZarrDimensionType_Space, 64, 16, 0); // 4 chunks

    try {
        EXPECT_EQ(zarr::tile_group_offset(0, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(1, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(2, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(3, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(4, dims), 24);
        EXPECT_EQ(zarr::tile_group_offset(5, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(6, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(7, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(8, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(9, dims), 24);
        EXPECT_EQ(zarr::tile_group_offset(10, dims), 36);
        EXPECT_EQ(zarr::tile_group_offset(11, dims), 36);
        EXPECT_EQ(zarr::tile_group_offset(12, dims), 48);
        EXPECT_EQ(zarr::tile_group_offset(13, dims), 48);
        EXPECT_EQ(zarr::tile_group_offset(14, dims), 60);
        EXPECT_EQ(zarr::tile_group_offset(15, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(16, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(17, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(18, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(19, dims), 24);
        EXPECT_EQ(zarr::tile_group_offset(20, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(21, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(22, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(23, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(24, dims), 24);
        EXPECT_EQ(zarr::tile_group_offset(25, dims), 36);
        EXPECT_EQ(zarr::tile_group_offset(26, dims), 36);
        EXPECT_EQ(zarr::tile_group_offset(27, dims), 48);
        EXPECT_EQ(zarr::tile_group_offset(28, dims), 48);
        EXPECT_EQ(zarr::tile_group_offset(29, dims), 60);
        EXPECT_EQ(zarr::tile_group_offset(30, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(31, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(32, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(33, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(34, dims), 24);
        EXPECT_EQ(zarr::tile_group_offset(35, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(36, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(37, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(38, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(39, dims), 24);
        EXPECT_EQ(zarr::tile_group_offset(40, dims), 36);
        EXPECT_EQ(zarr::tile_group_offset(41, dims), 36);
        EXPECT_EQ(zarr::tile_group_offset(42, dims), 48);
        EXPECT_EQ(zarr::tile_group_offset(43, dims), 48);
        EXPECT_EQ(zarr::tile_group_offset(44, dims), 60);
        EXPECT_EQ(zarr::tile_group_offset(45, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(46, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(47, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(48, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(49, dims), 24);
        EXPECT_EQ(zarr::tile_group_offset(50, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(51, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(52, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(53, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(54, dims), 24);
        EXPECT_EQ(zarr::tile_group_offset(55, dims), 36);
        EXPECT_EQ(zarr::tile_group_offset(56, dims), 36);
        EXPECT_EQ(zarr::tile_group_offset(57, dims), 48);
        EXPECT_EQ(zarr::tile_group_offset(58, dims), 48);
        EXPECT_EQ(zarr::tile_group_offset(59, dims), 60);
        EXPECT_EQ(zarr::tile_group_offset(60, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(61, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(62, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(63, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(64, dims), 24);
        EXPECT_EQ(zarr::tile_group_offset(65, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(66, dims), 0);
        EXPECT_EQ(zarr::tile_group_offset(67, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(68, dims), 12);
        EXPECT_EQ(zarr::tile_group_offset(69, dims), 24);
        EXPECT_EQ(zarr::tile_group_offset(70, dims), 36);
        EXPECT_EQ(zarr::tile_group_offset(71, dims), 36);
        EXPECT_EQ(zarr::tile_group_offset(72, dims), 48);
        EXPECT_EQ(zarr::tile_group_offset(73, dims), 48);
        EXPECT_EQ(zarr::tile_group_offset(74, dims), 60);
        EXPECT_EQ(zarr::tile_group_offset(75, dims), 0);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: %s\n", exc.what());
    } catch (...) {
        LOG_ERROR("Exception: (unknown)");
    }

    return retval;
}