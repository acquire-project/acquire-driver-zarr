#include "zarr.common.hh"
#include "unit.test.macros.hh"

#include <stdexcept>

#define EXPECT_INT_EQ(a, b)                                                    \
    EXPECT((a) == (b), "Expected %s == %s, but %zu != %zu", #a, #b, a, b)

int
main()
{
    int retval = 1;

    try {
        std::vector<zarr::Dimension> dims;
        dims.emplace_back("t",
                          ZarrDimensionType_Time,
                          0,
                          5,  // 5 timepoints / chunk
                          2); // 2 chunks / shard
        dims.emplace_back("c",
                          ZarrDimensionType_Channel,
                          8,
                          4,  // 8 / 4 = 2 chunks
                          2); // 4 / 2 = 2 shards
        dims.emplace_back("z",
                          ZarrDimensionType_Space,
                          6,
                          2,  // 6 / 2 = 3 chunks
                          1); // 3 / 1 = 3 shards
        dims.emplace_back("y",
                          ZarrDimensionType_Space,
                          48,
                          16, // 48 / 16 = 3 chunks
                          1); // 3 / 1 = 3 shards
        dims.emplace_back("x",
                          ZarrDimensionType_Space,
                          64,
                          16, // 64 / 16 = 4 chunks
                          2); // 4 / 2 = 2 shards

        EXPECT_INT_EQ(zarr::shard_index_for_chunk(0, dims), 0);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(1, dims), 0);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(2, dims), 1);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(3, dims), 1);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(4, dims), 2);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(5, dims), 2);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(6, dims), 3);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(7, dims), 3);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(8, dims), 4);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(9, dims), 4);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(10, dims), 5);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(11, dims), 5);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(12, dims), 6);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(13, dims), 6);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(14, dims), 7);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(15, dims), 7);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(16, dims), 8);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(17, dims), 8);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(18, dims), 9);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(19, dims), 9);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(20, dims), 10);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(21, dims), 10);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(22, dims), 11);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(23, dims), 11);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(24, dims), 12);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(25, dims), 12);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(26, dims), 13);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(27, dims), 13);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(28, dims), 14);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(29, dims), 14);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(30, dims), 15);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(31, dims), 15);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(32, dims), 16);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(33, dims), 16);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(34, dims), 17);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(35, dims), 17);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(36, dims), 0);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(37, dims), 0);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(38, dims), 1);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(39, dims), 1);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(40, dims), 2);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(41, dims), 2);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(42, dims), 3);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(43, dims), 3);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(44, dims), 4);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(45, dims), 4);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(46, dims), 5);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(47, dims), 5);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(48, dims), 6);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(49, dims), 6);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(50, dims), 7);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(51, dims), 7);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(52, dims), 8);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(53, dims), 8);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(54, dims), 9);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(55, dims), 9);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(56, dims), 10);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(57, dims), 10);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(58, dims), 11);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(59, dims), 11);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(60, dims), 12);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(61, dims), 12);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(62, dims), 13);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(63, dims), 13);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(64, dims), 14);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(65, dims), 14);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(66, dims), 15);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(67, dims), 15);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(68, dims), 16);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(69, dims), 16);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(70, dims), 17);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(71, dims), 17);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(72, dims), 0);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(73, dims), 0);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(74, dims), 1);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(75, dims), 1);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(76, dims), 2);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(77, dims), 2);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(78, dims), 3);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(79, dims), 3);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(80, dims), 4);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(81, dims), 4);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(82, dims), 5);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(83, dims), 5);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(84, dims), 6);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(85, dims), 6);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(86, dims), 7);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(87, dims), 7);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(88, dims), 8);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(89, dims), 8);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(90, dims), 9);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(91, dims), 9);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(92, dims), 10);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(93, dims), 10);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(94, dims), 11);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(95, dims), 11);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(96, dims), 12);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(97, dims), 12);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(98, dims), 13);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(99, dims), 13);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(100, dims), 14);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(101, dims), 14);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(102, dims), 15);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(103, dims), 15);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(104, dims), 16);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(105, dims), 16);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(106, dims), 17);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(107, dims), 17);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(108, dims), 0);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(109, dims), 0);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(110, dims), 1);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(111, dims), 1);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(112, dims), 2);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(113, dims), 2);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(114, dims), 3);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(115, dims), 3);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(116, dims), 4);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(117, dims), 4);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(118, dims), 5);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(119, dims), 5);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(120, dims), 6);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(121, dims), 6);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(122, dims), 7);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(123, dims), 7);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(124, dims), 8);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(125, dims), 8);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(126, dims), 9);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(127, dims), 9);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(128, dims), 10);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(129, dims), 10);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(130, dims), 11);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(131, dims), 11);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(132, dims), 12);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(133, dims), 12);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(134, dims), 13);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(135, dims), 13);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(136, dims), 14);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(137, dims), 14);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(138, dims), 15);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(139, dims), 15);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(140, dims), 16);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(141, dims), 16);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(142, dims), 17);
        EXPECT_INT_EQ(zarr::shard_index_for_chunk(143, dims), 17);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: %s\n", exc.what());
    } catch (...) {
        LOG_ERROR("Exception: (unknown)");
    }

    return retval;
}