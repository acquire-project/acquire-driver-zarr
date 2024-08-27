#include "zarr.common.hh"
#include "logger.hh"

#include <stdexcept>

#define EXPECT_EQ(a, b)                                                        \
    EXPECT((a) == (b), "Expected %s == %s, got %s == %s", #a, #b, a, b)

int
main()
{
    int retval = 1;

    try {
        std::vector<zarr::Dimension> dims;
        dims.emplace_back(
          "t", ZarrDimensionType_Time, 0, 5, 0); // 5 timepoints / chunk
        dims.emplace_back("c", ZarrDimensionType_Channel, 3, 2, 0); // 2 chunks
        dims.emplace_back("z", ZarrDimensionType_Space, 5, 2, 0);   // 3 chunks
        dims.emplace_back("y", ZarrDimensionType_Space, 48, 16, 0); // 3 chunks
        dims.emplace_back("x", ZarrDimensionType_Space, 64, 16, 0); // 4 chunks

        EXPECT_EQ(zarr::chunk_lattice_index(0, 2, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(0, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(0, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(1, 2, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(1, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(1, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(2, 2, dims), 1);
        EXPECT_EQ(zarr::chunk_lattice_index(2, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(2, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(3, 2, dims), 1);
        EXPECT_EQ(zarr::chunk_lattice_index(3, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(3, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(4, 2, dims), 2);
        EXPECT_EQ(zarr::chunk_lattice_index(4, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(4, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(5, 2, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(5, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(5, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(12, 2, dims), 1);
        EXPECT_EQ(zarr::chunk_lattice_index(12, 1, dims), 1);
        EXPECT_EQ(zarr::chunk_lattice_index(12, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(19, 2, dims), 2);
        EXPECT_EQ(zarr::chunk_lattice_index(19, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(19, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(26, 2, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(26, 1, dims), 1);
        EXPECT_EQ(zarr::chunk_lattice_index(26, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(33, 2, dims), 1);
        EXPECT_EQ(zarr::chunk_lattice_index(33, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(33, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(40, 2, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(40, 1, dims), 1);
        EXPECT_EQ(zarr::chunk_lattice_index(40, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(47, 2, dims), 1);
        EXPECT_EQ(zarr::chunk_lattice_index(47, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(47, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(54, 2, dims), 2);
        EXPECT_EQ(zarr::chunk_lattice_index(54, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(54, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(61, 2, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(61, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(61, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(68, 2, dims), 1);
        EXPECT_EQ(zarr::chunk_lattice_index(68, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(68, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(74, 2, dims), 2);
        EXPECT_EQ(zarr::chunk_lattice_index(74, 1, dims), 1);
        EXPECT_EQ(zarr::chunk_lattice_index(74, 0, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(75, 2, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(75, 1, dims), 0);
        EXPECT_EQ(zarr::chunk_lattice_index(75, 0, dims), 1);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: %s\n", exc.what());
    } catch (...) {
        LOG_ERROR("Exception: (unknown)");
    }

    return retval;
}