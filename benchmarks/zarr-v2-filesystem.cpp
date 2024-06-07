#include "device/props/storage.h"

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "benchmark.storage.hh"

#define CHECK(e)                                                               \
    if (!(e)) {                                                                \
        std::stringstream ss;                                                  \
        ss << "Expression failed on line " << __LINE__ << ": " << #e;          \
        throw std::runtime_error(ss.str());                                    \
    }

int
main()
{
    StorageProperties props{};

    std::vector<size_t> frame_widths = { 128, 256, 512, 1024, 2048, 4096 };
    std::vector<size_t> chunk_widths = { 128, 256, 512, 1024, 2048, 4096 };
    std::vector<size_t> frame_heights = { 128, 256, 512, 1024, 2048, 4096 };
    std::vector<size_t> chunk_heights = { 128, 256, 512, 1024, 2048, 4096 };
    std::vector<size_t> chunk_planes = { 2, 5, 10, 20, 25 };

    std::vector<StorageProperties> props_vec;
    for (const auto& frame_width : frame_widths) {
        for (const auto& chunk_width : chunk_widths) {
            if (chunk_width > frame_width) {
                continue;
            }

            for (const auto& frame_height : frame_heights) {
                for (const auto& chunk_height : chunk_heights) {
                    if (chunk_height > frame_height) {
                        continue;
                    }

                    for (const auto& chunk_plane : chunk_planes) {
                        const std::string filename =
                          BENCHMARK + std::to_string(frame_width) + "-" +
                          std::to_string(chunk_width) + "-" +
                          std::to_string(frame_height) + "-" +
                          std::to_string(chunk_height) + "-" +
                          std::to_string(chunk_plane) + ".zarr";

                        storage_properties_init(&props,
                                                0,
                                                filename.c_str(),
                                                filename.size() + 1,
                                                nullptr,
                                                0,
                                                {},
                                                3);

                        CHECK(
                          storage_properties_set_dimension(&props,
                                                           0,
                                                           "x",
                                                           sizeof("x"),
                                                           DimensionType_Space,
                                                           frame_width,
                                                           chunk_width,
                                                           0));
                        CHECK(
                          storage_properties_set_dimension(&props,
                                                           1,
                                                           "y",
                                                           sizeof("y"),
                                                           DimensionType_Space,
                                                           frame_height,
                                                           chunk_height,
                                                           0));
                        CHECK(
                          storage_properties_set_dimension(&props,
                                                           2,
                                                           "t",
                                                           sizeof("t"),
                                                           DimensionType_Time,
                                                           0,
                                                           chunk_plane,
                                                           0));

                        props_vec.push_back(props);
                    }
                }
            }
        }
    }

    try {
        benchmark_storage("Zarr", props_vec);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 1;
}