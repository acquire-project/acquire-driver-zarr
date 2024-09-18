#include "zarr.common.hh"
#include "logger.hh"

#include <stdexcept>

#define EXPECT_EQ(a, b)                                                        \
    EXPECT((a) == (b), "Expected %s == %s, but %zu != %zu", #a, #b, a, b)

int main() {
    int retval = 1;

    std::vector<zarr::Dimension> dims;
    dims.emplace_back(
      "t", ZarrDimensionType_Time, 0, 5, 0); // 5 timepoints / chunk
    dims.emplace_back("c", ZarrDimensionType_Channel, 3, 2, 0); // 2 chunks
    dims.emplace_back("z", ZarrDimensionType_Space, 5, 2, 0);   // 3 chunks
    dims.emplace_back("y", ZarrDimensionType_Space, 48, 16, 0); // 3 chunks
    dims.emplace_back("x", ZarrDimensionType_Space, 64, 16, 0); // 4 chunks

    try {
        EXPECT_EQ(zarr::chunk_internal_offset(0, dims, ZarrDataType_uint16), 0);
        EXPECT_EQ(zarr::chunk_internal_offset(1, dims, ZarrDataType_uint16), 512);
        EXPECT_EQ(zarr::chunk_internal_offset(2, dims, ZarrDataType_uint16), 0);
        EXPECT_EQ(zarr::chunk_internal_offset(3, dims, ZarrDataType_uint16), 512);
        EXPECT_EQ(zarr::chunk_internal_offset(4, dims, ZarrDataType_uint16), 0);
        EXPECT_EQ(zarr::chunk_internal_offset(5, dims, ZarrDataType_uint16), 1024);
        EXPECT_EQ(zarr::chunk_internal_offset(6, dims, ZarrDataType_uint16), 1536);
        EXPECT_EQ(zarr::chunk_internal_offset(7, dims, ZarrDataType_uint16), 1024);
        EXPECT_EQ(zarr::chunk_internal_offset(8, dims, ZarrDataType_uint16), 1536);
        EXPECT_EQ(zarr::chunk_internal_offset(9, dims, ZarrDataType_uint16), 1024);
        EXPECT_EQ(zarr::chunk_internal_offset(10, dims, ZarrDataType_uint16), 0);
        EXPECT_EQ(zarr::chunk_internal_offset(11, dims, ZarrDataType_uint16), 512);
        EXPECT_EQ(zarr::chunk_internal_offset(12, dims, ZarrDataType_uint16), 0);
        EXPECT_EQ(zarr::chunk_internal_offset(13, dims, ZarrDataType_uint16), 512);
        EXPECT_EQ(zarr::chunk_internal_offset(14, dims, ZarrDataType_uint16), 0);
        EXPECT_EQ(zarr::chunk_internal_offset(15, dims, ZarrDataType_uint16), 2048);
        EXPECT_EQ(zarr::chunk_internal_offset(16, dims, ZarrDataType_uint16), 2560);
        EXPECT_EQ(zarr::chunk_internal_offset(17, dims, ZarrDataType_uint16), 2048);
        EXPECT_EQ(zarr::chunk_internal_offset(18, dims, ZarrDataType_uint16), 2560);
        EXPECT_EQ(zarr::chunk_internal_offset(19, dims, ZarrDataType_uint16), 2048);
        EXPECT_EQ(zarr::chunk_internal_offset(20, dims, ZarrDataType_uint16), 3072);
        EXPECT_EQ(zarr::chunk_internal_offset(21, dims, ZarrDataType_uint16), 3584);
        EXPECT_EQ(zarr::chunk_internal_offset(22, dims, ZarrDataType_uint16), 3072);
        EXPECT_EQ(zarr::chunk_internal_offset(23, dims, ZarrDataType_uint16), 3584);
        EXPECT_EQ(zarr::chunk_internal_offset(24, dims, ZarrDataType_uint16), 3072);
        EXPECT_EQ(zarr::chunk_internal_offset(25, dims, ZarrDataType_uint16), 2048);
        EXPECT_EQ(zarr::chunk_internal_offset(26, dims, ZarrDataType_uint16), 2560);
        EXPECT_EQ(zarr::chunk_internal_offset(27, dims, ZarrDataType_uint16), 2048);
        EXPECT_EQ(zarr::chunk_internal_offset(28, dims, ZarrDataType_uint16), 2560);
        EXPECT_EQ(zarr::chunk_internal_offset(29, dims, ZarrDataType_uint16), 2048);
        EXPECT_EQ(zarr::chunk_internal_offset(30, dims, ZarrDataType_uint16), 4096);
        EXPECT_EQ(zarr::chunk_internal_offset(31, dims, ZarrDataType_uint16), 4608);
        EXPECT_EQ(zarr::chunk_internal_offset(32, dims, ZarrDataType_uint16), 4096);
        EXPECT_EQ(zarr::chunk_internal_offset(33, dims, ZarrDataType_uint16), 4608);
        EXPECT_EQ(zarr::chunk_internal_offset(34, dims, ZarrDataType_uint16), 4096);
        EXPECT_EQ(zarr::chunk_internal_offset(35, dims, ZarrDataType_uint16), 5120);
        EXPECT_EQ(zarr::chunk_internal_offset(36, dims, ZarrDataType_uint16), 5632);
        EXPECT_EQ(zarr::chunk_internal_offset(37, dims, ZarrDataType_uint16), 5120);
        EXPECT_EQ(zarr::chunk_internal_offset(38, dims, ZarrDataType_uint16), 5632);
        EXPECT_EQ(zarr::chunk_internal_offset(39, dims, ZarrDataType_uint16), 5120);
        EXPECT_EQ(zarr::chunk_internal_offset(40, dims, ZarrDataType_uint16), 4096);
        EXPECT_EQ(zarr::chunk_internal_offset(41, dims, ZarrDataType_uint16), 4608);
        EXPECT_EQ(zarr::chunk_internal_offset(42, dims, ZarrDataType_uint16), 4096);
        EXPECT_EQ(zarr::chunk_internal_offset(43, dims, ZarrDataType_uint16), 4608);
        EXPECT_EQ(zarr::chunk_internal_offset(44, dims, ZarrDataType_uint16), 4096);
        EXPECT_EQ(zarr::chunk_internal_offset(45, dims, ZarrDataType_uint16), 6144);
        EXPECT_EQ(zarr::chunk_internal_offset(46, dims, ZarrDataType_uint16), 6656);
        EXPECT_EQ(zarr::chunk_internal_offset(47, dims, ZarrDataType_uint16), 6144);
        EXPECT_EQ(zarr::chunk_internal_offset(48, dims, ZarrDataType_uint16), 6656);
        EXPECT_EQ(zarr::chunk_internal_offset(49, dims, ZarrDataType_uint16), 6144);
        EXPECT_EQ(zarr::chunk_internal_offset(50, dims, ZarrDataType_uint16), 7168);
        EXPECT_EQ(zarr::chunk_internal_offset(51, dims, ZarrDataType_uint16), 7680);
        EXPECT_EQ(zarr::chunk_internal_offset(52, dims, ZarrDataType_uint16), 7168);
        EXPECT_EQ(zarr::chunk_internal_offset(53, dims, ZarrDataType_uint16), 7680);
        EXPECT_EQ(zarr::chunk_internal_offset(54, dims, ZarrDataType_uint16), 7168);
        EXPECT_EQ(zarr::chunk_internal_offset(55, dims, ZarrDataType_uint16), 6144);
        EXPECT_EQ(zarr::chunk_internal_offset(56, dims, ZarrDataType_uint16), 6656);
        EXPECT_EQ(zarr::chunk_internal_offset(57, dims, ZarrDataType_uint16), 6144);
        EXPECT_EQ(zarr::chunk_internal_offset(58, dims, ZarrDataType_uint16), 6656);
        EXPECT_EQ(zarr::chunk_internal_offset(59, dims, ZarrDataType_uint16), 6144);
        EXPECT_EQ(zarr::chunk_internal_offset(60, dims, ZarrDataType_uint16), 8192);
        EXPECT_EQ(zarr::chunk_internal_offset(61, dims, ZarrDataType_uint16), 8704);
        EXPECT_EQ(zarr::chunk_internal_offset(62, dims, ZarrDataType_uint16), 8192);
        EXPECT_EQ(zarr::chunk_internal_offset(63, dims, ZarrDataType_uint16), 8704);
        EXPECT_EQ(zarr::chunk_internal_offset(64, dims, ZarrDataType_uint16), 8192);
        EXPECT_EQ(zarr::chunk_internal_offset(65, dims, ZarrDataType_uint16), 9216);
        EXPECT_EQ(zarr::chunk_internal_offset(66, dims, ZarrDataType_uint16), 9728);
        EXPECT_EQ(zarr::chunk_internal_offset(67, dims, ZarrDataType_uint16), 9216);
        EXPECT_EQ(zarr::chunk_internal_offset(68, dims, ZarrDataType_uint16), 9728);
        EXPECT_EQ(zarr::chunk_internal_offset(69, dims, ZarrDataType_uint16), 9216);
        EXPECT_EQ(zarr::chunk_internal_offset(70, dims, ZarrDataType_uint16), 8192);
        EXPECT_EQ(zarr::chunk_internal_offset(71, dims, ZarrDataType_uint16), 8704);
        EXPECT_EQ(zarr::chunk_internal_offset(72, dims, ZarrDataType_uint16), 8192);
        EXPECT_EQ(zarr::chunk_internal_offset(73, dims, ZarrDataType_uint16), 8704);
        EXPECT_EQ(zarr::chunk_internal_offset(74, dims, ZarrDataType_uint16), 8192);
        EXPECT_EQ(zarr::chunk_internal_offset(75, dims, ZarrDataType_uint16), 0);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: %s\n", exc.what());
    } catch (...) {
        LOG_ERROR("Exception: (unknown)");
    }

    return retval;
}