#include "zarrv3.writer.hh"
#include "sink.creator.hh"
#include "../zarr.hh"

#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

namespace {
/// @brief Get the shard index for a given chunk index.
size_t
shard_index(size_t chunk_idx, const std::vector<zarr::Dimension>& dimensions)
{
    // make chunk strides
    std::vector<size_t> chunk_strides(1, 1);
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        chunk_strides.push_back(chunk_strides.back() *
                                zarr::common::chunks_along_dimension(dim));
        CHECK(chunk_strides.back());
    }

    // get chunk indices
    std::vector<size_t> chunk_lattice_indices;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        chunk_lattice_indices.push_back(chunk_idx % chunk_strides.at(i + 1) /
                                        chunk_strides.at(i));
    }
    chunk_lattice_indices.push_back(chunk_idx / chunk_strides.back());

    // make shard strides
    std::vector<size_t> shard_strides(1, 1);
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        shard_strides.push_back(shard_strides.back() *
                                zarr::common::shards_along_dimension(dim));
    }

    std::vector<size_t> shard_lattice_indices;
    for (auto i = 0; i < dimensions.size(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices.at(i) /
                                        dimensions.at(i).shard_size_chunks);
    }

    size_t index = 0;
    for (auto i = 0; i < dimensions.size(); ++i) {
        index += shard_lattice_indices.at(i) * shard_strides.at(i);
    }

    return index;
}

/// @brief Get the index for a chunk within a shard.
size_t
shard_internal_index(size_t chunk_idx,
                     const std::vector<zarr::Dimension>& dimensions)
{
    // make chunk strides
    std::vector<size_t> chunk_strides(1, 1);
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        chunk_strides.push_back(chunk_strides.back() *
                                zarr::common::chunks_along_dimension(dim));
        CHECK(chunk_strides.back());
    }

    // get chunk indices
    std::vector<size_t> chunk_lattice_indices;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        chunk_lattice_indices.push_back(chunk_idx % chunk_strides.at(i + 1) /
                                        chunk_strides.at(i));
    }
    chunk_lattice_indices.push_back(chunk_idx / chunk_strides.back());

    // make shard lattice indices
    std::vector<size_t> shard_lattice_indices;
    for (auto i = 0; i < dimensions.size(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices.at(i) /
                                        dimensions.at(i).shard_size_chunks);
    }

    std::vector<size_t> chunk_internal_strides(1, 1);
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        chunk_internal_strides.push_back(chunk_internal_strides.back() *
                                         dim.shard_size_chunks);
    }

    size_t index = 0;

    for (auto i = 0; i < dimensions.size(); ++i) {
        index +=
          (chunk_lattice_indices.at(i) % dimensions.at(i).shard_size_chunks) *
          chunk_internal_strides.at(i);
    }

    return index;
}
} // namespace

zarr::ZarrV3Writer::ZarrV3Writer(
  const WriterConfig& array_spec,
  std::shared_ptr<common::ThreadPool> thread_pool)
  : Writer(array_spec, thread_pool)
  , shard_file_offsets_(common::number_of_shards(array_spec.dimensions), 0)
  , shard_tables_{ common::number_of_shards(array_spec.dimensions) }
{
    const auto chunks_per_shard =
      common::chunks_per_shard(array_spec.dimensions);

    for (auto& table : shard_tables_) {
        table.resize(2 * chunks_per_shard);
        std::fill_n(
          table.begin(), table.size(), std::numeric_limits<uint64_t>::max());
    }
}

bool
zarr::ZarrV3Writer::flush_impl_()
{
    // create shard files if they don't exist
    const std::string data_root =
      (fs::path(data_root_) / ("c" + std::to_string(append_chunk_index_)))
        .string();

    {
        SinkCreator creator(thread_pool_);
        if (sinks_.empty() &&
            !creator.make_data_sinks(data_root,
                                     config_.dimensions,
                                     common::shards_along_dimension,
                                     sinks_)) {
            return false;
        }
    }

    const auto n_shards = common::number_of_shards(config_.dimensions);
    CHECK(sinks_.size() == n_shards);

    // get shard indices for each chunk
    std::vector<std::vector<size_t>> chunk_in_shards(n_shards);
    for (auto i = 0; i < chunk_buffers_.size(); ++i) {
        const auto index = shard_index(i, config_.dimensions);
        chunk_in_shards.at(index).push_back(i);
    }

    // write out chunks to shards
    bool write_table = is_finalizing_ || should_rollover_();
    std::latch latch(n_shards);
    for (auto i = 0; i < n_shards; ++i) {
        const auto& chunks = chunk_in_shards.at(i);
        auto& chunk_table = shard_tables_.at(i);
        size_t* file_offset = &shard_file_offsets_.at(i);

        thread_pool_->push_to_job_queue([&sink = sinks_.at(i),
                                         &chunks,
                                         &chunk_table,
                                         file_offset,
                                         write_table,
                                         &latch,
                                         this](std::string& err) mutable {
            bool success = false;

            try {
                for (const auto& chunk_idx : chunks) {
                    auto& chunk = chunk_buffers_.at(chunk_idx);

                    success =
                      sink->write(*file_offset, chunk.data(), chunk.size());
                    if (!success) {
                        break;
                    }

                    const auto internal_idx =
                      shard_internal_index(chunk_idx, config_.dimensions);
                    chunk_table.at(2 * internal_idx) = *file_offset;
                    chunk_table.at(2 * internal_idx + 1) = chunk.size();

                    *file_offset += chunk.size();
                }

                if (success && write_table) {
                    const auto* table =
                      reinterpret_cast<const uint8_t*>(chunk_table.data());
                    success =
                      sink->write(*file_offset,
                                  table,
                                  chunk_table.size() * sizeof(uint64_t));
                }
            } catch (const std::exception& exc) {
                err = "Failed to write chunk: " + std::string(exc.what());
            } catch (...) {
                err = "Failed to write chunk: (unknown)";
            }

            latch.count_down();
            return success;
        });
    }

    // wait for all threads to finish
    latch.wait();

    // reset shard tables and file offsets
    if (write_table) {
        for (auto& table : shard_tables_) {
            std::fill_n(table.begin(),
                        table.size(),
                        std::numeric_limits<uint64_t>::max());
        }

        std::fill_n(shard_file_offsets_.begin(), shard_file_offsets_.size(), 0);
    }

    return true;
}

bool
zarr::ZarrV3Writer::should_rollover_() const
{
    const auto& dims = config_.dimensions;
    size_t frames_before_flush =
      dims.back().chunk_size_px * dims.back().shard_size_chunks;
    for (auto i = 2; i < dims.size() - 1; ++i) {
        frames_before_flush *= dims[i].array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_ % frames_before_flush == 0;
}

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

namespace common = zarr::common;

extern "C"
{
    acquire_export int unit_test__shard_index()
    {
        int retval = 0;
        try {
            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x",
                              DimensionType_Space,
                              64,
                              16, // 64 / 16 = 4 chunks
                              2); // 4 / 2 = 2 shards
            dims.emplace_back("y",
                              DimensionType_Space,
                              48,
                              16, // 48 / 16 = 3 chunks
                              1); // 3 / 1 = 3 shards
            dims.emplace_back("z",
                              DimensionType_Space,
                              6,
                              2,  // 6 / 2 = 3 chunks
                              1); // 3 / 1 = 3 shards
            dims.emplace_back("c",
                              DimensionType_Channel,
                              8,
                              4,  // 8 / 4 = 2 chunks
                              2); // 4 / 2 = 2 shards
            dims.emplace_back("t",
                              DimensionType_Time,
                              0,
                              5,  // 5 timepoints / chunk
                              2); // 2 chunks / shard

            CHECK(shard_index(0, dims) == 0);
            CHECK(shard_index(1, dims) == 0);
            CHECK(shard_index(2, dims) == 1);
            CHECK(shard_index(3, dims) == 1);
            CHECK(shard_index(4, dims) == 2);
            CHECK(shard_index(5, dims) == 2);
            CHECK(shard_index(6, dims) == 3);
            CHECK(shard_index(7, dims) == 3);
            CHECK(shard_index(8, dims) == 4);
            CHECK(shard_index(9, dims) == 4);
            CHECK(shard_index(10, dims) == 5);
            CHECK(shard_index(11, dims) == 5);
            CHECK(shard_index(12, dims) == 6);
            CHECK(shard_index(13, dims) == 6);
            CHECK(shard_index(14, dims) == 7);
            CHECK(shard_index(15, dims) == 7);
            CHECK(shard_index(16, dims) == 8);
            CHECK(shard_index(17, dims) == 8);
            CHECK(shard_index(18, dims) == 9);
            CHECK(shard_index(19, dims) == 9);
            CHECK(shard_index(20, dims) == 10);
            CHECK(shard_index(21, dims) == 10);
            CHECK(shard_index(22, dims) == 11);
            CHECK(shard_index(23, dims) == 11);
            CHECK(shard_index(24, dims) == 12);
            CHECK(shard_index(25, dims) == 12);
            CHECK(shard_index(26, dims) == 13);
            CHECK(shard_index(27, dims) == 13);
            CHECK(shard_index(28, dims) == 14);
            CHECK(shard_index(29, dims) == 14);
            CHECK(shard_index(30, dims) == 15);
            CHECK(shard_index(31, dims) == 15);
            CHECK(shard_index(32, dims) == 16);
            CHECK(shard_index(33, dims) == 16);
            CHECK(shard_index(34, dims) == 17);
            CHECK(shard_index(35, dims) == 17);
            CHECK(shard_index(36, dims) == 0);
            CHECK(shard_index(37, dims) == 0);
            CHECK(shard_index(38, dims) == 1);
            CHECK(shard_index(39, dims) == 1);
            CHECK(shard_index(40, dims) == 2);
            CHECK(shard_index(41, dims) == 2);
            CHECK(shard_index(42, dims) == 3);
            CHECK(shard_index(43, dims) == 3);
            CHECK(shard_index(44, dims) == 4);
            CHECK(shard_index(45, dims) == 4);
            CHECK(shard_index(46, dims) == 5);
            CHECK(shard_index(47, dims) == 5);
            CHECK(shard_index(48, dims) == 6);
            CHECK(shard_index(49, dims) == 6);
            CHECK(shard_index(50, dims) == 7);
            CHECK(shard_index(51, dims) == 7);
            CHECK(shard_index(52, dims) == 8);
            CHECK(shard_index(53, dims) == 8);
            CHECK(shard_index(54, dims) == 9);
            CHECK(shard_index(55, dims) == 9);
            CHECK(shard_index(56, dims) == 10);
            CHECK(shard_index(57, dims) == 10);
            CHECK(shard_index(58, dims) == 11);
            CHECK(shard_index(59, dims) == 11);
            CHECK(shard_index(60, dims) == 12);
            CHECK(shard_index(61, dims) == 12);
            CHECK(shard_index(62, dims) == 13);
            CHECK(shard_index(63, dims) == 13);
            CHECK(shard_index(64, dims) == 14);
            CHECK(shard_index(65, dims) == 14);
            CHECK(shard_index(66, dims) == 15);
            CHECK(shard_index(67, dims) == 15);
            CHECK(shard_index(68, dims) == 16);
            CHECK(shard_index(69, dims) == 16);
            CHECK(shard_index(70, dims) == 17);
            CHECK(shard_index(71, dims) == 17);
            CHECK(shard_index(72, dims) == 0);
            CHECK(shard_index(73, dims) == 0);
            CHECK(shard_index(74, dims) == 1);
            CHECK(shard_index(75, dims) == 1);
            CHECK(shard_index(76, dims) == 2);
            CHECK(shard_index(77, dims) == 2);
            CHECK(shard_index(78, dims) == 3);
            CHECK(shard_index(79, dims) == 3);
            CHECK(shard_index(80, dims) == 4);
            CHECK(shard_index(81, dims) == 4);
            CHECK(shard_index(82, dims) == 5);
            CHECK(shard_index(83, dims) == 5);
            CHECK(shard_index(84, dims) == 6);
            CHECK(shard_index(85, dims) == 6);
            CHECK(shard_index(86, dims) == 7);
            CHECK(shard_index(87, dims) == 7);
            CHECK(shard_index(88, dims) == 8);
            CHECK(shard_index(89, dims) == 8);
            CHECK(shard_index(90, dims) == 9);
            CHECK(shard_index(91, dims) == 9);
            CHECK(shard_index(92, dims) == 10);
            CHECK(shard_index(93, dims) == 10);
            CHECK(shard_index(94, dims) == 11);
            CHECK(shard_index(95, dims) == 11);
            CHECK(shard_index(96, dims) == 12);
            CHECK(shard_index(97, dims) == 12);
            CHECK(shard_index(98, dims) == 13);
            CHECK(shard_index(99, dims) == 13);
            CHECK(shard_index(100, dims) == 14);
            CHECK(shard_index(101, dims) == 14);
            CHECK(shard_index(102, dims) == 15);
            CHECK(shard_index(103, dims) == 15);
            CHECK(shard_index(104, dims) == 16);
            CHECK(shard_index(105, dims) == 16);
            CHECK(shard_index(106, dims) == 17);
            CHECK(shard_index(107, dims) == 17);
            CHECK(shard_index(108, dims) == 0);
            CHECK(shard_index(109, dims) == 0);
            CHECK(shard_index(110, dims) == 1);
            CHECK(shard_index(111, dims) == 1);
            CHECK(shard_index(112, dims) == 2);
            CHECK(shard_index(113, dims) == 2);
            CHECK(shard_index(114, dims) == 3);
            CHECK(shard_index(115, dims) == 3);
            CHECK(shard_index(116, dims) == 4);
            CHECK(shard_index(117, dims) == 4);
            CHECK(shard_index(118, dims) == 5);
            CHECK(shard_index(119, dims) == 5);
            CHECK(shard_index(120, dims) == 6);
            CHECK(shard_index(121, dims) == 6);
            CHECK(shard_index(122, dims) == 7);
            CHECK(shard_index(123, dims) == 7);
            CHECK(shard_index(124, dims) == 8);
            CHECK(shard_index(125, dims) == 8);
            CHECK(shard_index(126, dims) == 9);
            CHECK(shard_index(127, dims) == 9);
            CHECK(shard_index(128, dims) == 10);
            CHECK(shard_index(129, dims) == 10);
            CHECK(shard_index(130, dims) == 11);
            CHECK(shard_index(131, dims) == 11);
            CHECK(shard_index(132, dims) == 12);
            CHECK(shard_index(133, dims) == 12);
            CHECK(shard_index(134, dims) == 13);
            CHECK(shard_index(135, dims) == 13);
            CHECK(shard_index(136, dims) == 14);
            CHECK(shard_index(137, dims) == 14);
            CHECK(shard_index(138, dims) == 15);
            CHECK(shard_index(139, dims) == 15);
            CHECK(shard_index(140, dims) == 16);
            CHECK(shard_index(141, dims) == 16);
            CHECK(shard_index(142, dims) == 17);
            CHECK(shard_index(143, dims) == 17);

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        return retval;
    }

    acquire_export int unit_test__shard_internal_index()
    {
        int retval = 0;

        std::vector<zarr::Dimension> dims;
        dims.emplace_back("x",
                          DimensionType_Space,
                          1080,
                          270, // 4 chunks
                          3);  // 2 ragged shards
        dims.emplace_back("y",
                          DimensionType_Space,
                          960,
                          320, // 3 chunks
                          2);  // 2 ragged shards
        dims.emplace_back("t",
                          DimensionType_Time,
                          0,
                          32, // 32 timepoints / chunk
                          1); // 1 shard

        try {
            CHECK(shard_index(0, dims) == 0);
            CHECK(shard_internal_index(0, dims) == 0);

            CHECK(shard_index(1, dims) == 0);
            CHECK(shard_internal_index(1, dims) == 1);

            CHECK(shard_index(2, dims) == 0);
            CHECK(shard_internal_index(2, dims) == 2);

            CHECK(shard_index(3, dims) == 1);
            CHECK(shard_internal_index(3, dims) == 0);

            CHECK(shard_index(4, dims) == 0);
            CHECK(shard_internal_index(4, dims) == 3);

            CHECK(shard_index(5, dims) == 0);
            CHECK(shard_internal_index(5, dims) == 4);

            CHECK(shard_index(6, dims) == 0);
            CHECK(shard_internal_index(6, dims) == 5);

            CHECK(shard_index(7, dims) == 1);
            CHECK(shard_internal_index(7, dims) == 3);

            CHECK(shard_index(8, dims) == 2);
            CHECK(shard_internal_index(8, dims) == 0);

            CHECK(shard_index(9, dims) == 2);
            CHECK(shard_internal_index(9, dims) == 1);

            CHECK(shard_index(10, dims) == 2);
            CHECK(shard_internal_index(10, dims) == 2);

            CHECK(shard_index(11, dims) == 3);
            CHECK(shard_internal_index(11, dims) == 0);
            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        return retval;
    }

    acquire_export int unit_test__zarrv3_writer__write_even()
    {
        int retval = 0;
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        struct VideoFrame* frame = nullptr;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x",
                              DimensionType_Space,
                              64,
                              16, // 64 / 16 = 4 chunks
                              2); // 4 / 2 = 2 shards
            dims.emplace_back("y",
                              DimensionType_Space,
                              48,
                              16, // 48 / 16 = 3 chunks
                              1); // 3 / 1 = 3 shards
            dims.emplace_back("z",
                              DimensionType_Space,
                              6,
                              2,  // 6 / 2 = 3 chunks
                              1); // 3 / 1 = 3 shards
            dims.emplace_back("c",
                              DimensionType_Channel,
                              8,
                              4,  // 8 / 4 = 2 chunks
                              2); // 4 / 2 = 2 shards
            dims.emplace_back("t",
                              DimensionType_Time,
                              0,
                              5,  // 5 timepoints / chunk
                              2); // 2 chunks / shard

            ImageShape shape {
                .dims = {
                  .width = 64,
                  .height = 48,
                },
                .strides = {
                  .channels = 1,
                  .width = 1,
                  .height = 64,
                  .planes = 64 * 48
                },
                .type = SampleType_u16,
            };

            zarr::WriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3Writer writer(config, thread_pool);

            const size_t frame_size = 64 * 48 * 2;

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + frame_size);
            frame->bytes_of_frame =
              common::align_up(sizeof(VideoFrame) + frame_size, 8);
            frame->shape = shape;
            memset(frame->data, 0, frame_size);

            for (auto i = 0; i < 6 * 8 * 5 * 2; ++i) {
                frame->frame_id = i;
                CHECK(writer.write(frame));
            }
            writer.finalize();

            const auto chunk_size = 16 *               // x
                                    16 *               // y
                                    2 *                // z
                                    4 *                // c
                                    5 *                // t
                                    2;                 // bytes per pixel
            const auto index_size = 8 *                // 8 chunks
                                    sizeof(uint64_t) * // indices are 64 bits
                                    2;                 // 2 indices per chunk
            const auto expected_file_size = 2 *        // x
                                              1 *      // y
                                              1 *      // z
                                              2 *      // c
                                              2 *      // t
                                              chunk_size +
                                            index_size;

            CHECK(fs::is_directory(base_dir));
            for (auto t = 0; t < 1; ++t) {
                const auto t_dir = base_dir / ("c" + std::to_string(t));
                CHECK(fs::is_directory(t_dir));

                for (auto c = 0; c < 1; ++c) {
                    const auto c_dir = t_dir / std::to_string(c);
                    CHECK(fs::is_directory(c_dir));

                    for (auto z = 0; z < 3; ++z) {
                        const auto z_dir = c_dir / std::to_string(z);
                        CHECK(fs::is_directory(z_dir));

                        for (auto y = 0; y < 3; ++y) {
                            const auto y_dir = z_dir / std::to_string(y);
                            CHECK(fs::is_directory(y_dir));

                            for (auto x = 0; x < 2; ++x) {
                                const auto x_file = y_dir / std::to_string(x);
                                CHECK(fs::is_regular_file(x_file));
                                const auto file_size = fs::file_size(x_file);
                                CHECK(file_size == expected_file_size);
                            }

                            CHECK(!fs::is_regular_file(y_dir / "2"));
                        }

                        CHECK(!fs::is_directory(z_dir / "3"));
                    }

                    CHECK(!fs::is_directory(c_dir / "3"));
                }

                CHECK(!fs::is_directory(t_dir / "1"));
            }

            CHECK(!fs::is_directory(base_dir / "c1"));

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        // cleanup
        if (fs::exists(base_dir)) {
            fs::remove_all(base_dir);
        }
        if (frame) {
            free(frame);
        }
        return retval;
    }

    acquire_export int unit_test__zarrv3_writer__write_ragged_append_dim()
    {
        int retval = 0;
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        struct VideoFrame* frame = nullptr;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x",
                              DimensionType_Space,
                              64,
                              16, // 64 / 16 = 4 chunks
                              2); // 4 / 2 = 2 shards
            dims.emplace_back("y",
                              DimensionType_Space,
                              48,
                              16, // 48 / 16 = 3 chunks
                              1); // 3 / 1 = 3 shards
            dims.emplace_back("z",
                              DimensionType_Space,
                              5,
                              2,  // 3 chunks, ragged
                              1); // 3 / 1 = 3 shards

            ImageShape shape {
                .dims = {
                  .width = 64,
                  .height = 48,
                },
                .strides = {
                  .channels = 1,
                  .width = 1,
                  .height = 64,
                  .planes = 64 * 48
                },
                .type = SampleType_u8,
            };

            zarr::WriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3Writer writer(config, thread_pool);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48);
            frame->bytes_of_frame =
              common::align_up(sizeof(VideoFrame) + 64 * 48, 8);
            frame->shape = shape;
            memset(frame->data, 0, 64 * 48);

            for (auto i = 0; i < 5; ++i) {
                frame->frame_id = i;
                CHECK(writer.write(frame));
            }
            writer.finalize();

            const auto chunk_size = 16 *               // x
                                    16 *               // y
                                    2;                 // z
            const auto index_size = 2 *                // 2 chunks
                                    sizeof(uint64_t) * // indices are 64 bits
                                    2;                 // 2 indices per chunk
            const auto expected_file_size = 2 *        // x
                                              1 *      // y
                                              1 *      // z
                                              chunk_size +
                                            index_size;

            CHECK(fs::is_directory(base_dir));
            for (auto z = 0; z < 3; ++z) {
                const auto z_dir = base_dir / ("c" + std::to_string(z));
                CHECK(fs::is_directory(z_dir));

                for (auto y = 0; y < 3; ++y) {
                    const auto y_dir = z_dir / std::to_string(y);
                    CHECK(fs::is_directory(y_dir));

                    for (auto x = 0; x < 2; ++x) {
                        const auto x_file = y_dir / std::to_string(x);
                        CHECK(fs::is_regular_file(x_file));
                        const auto file_size = fs::file_size(x_file);
                        CHECK(file_size == expected_file_size);
                    }

                    CHECK(!fs::is_regular_file(y_dir / "2"));
                }

                CHECK(!fs::is_directory(z_dir / "3"));
            }

            CHECK(!fs::is_directory(base_dir / "c3"));

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        // cleanup
        if (fs::exists(base_dir)) {
            fs::remove_all(base_dir);
        }
        if (frame) {
            free(frame);
        }
        return retval;
    }

    acquire_export int unit_test__zarrv3_writer__write_ragged_internal_dim()
    {
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        struct VideoFrame* frame = nullptr;
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            ImageShape shape {
                .dims = {
                  .width = 64,
                  .height = 48,
                },
                .strides = {
                  .channels = 1,
                  .width = 1,
                  .height = 64,
                  .planes = 64 * 48
                },
                .type = SampleType_u8,
            };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x",
                              DimensionType_Space,
                              64,
                              16, // 64 / 16 = 4 chunks
                              2); // 4 / 2 = 2 shards
            dims.emplace_back("y",
                              DimensionType_Space,
                              48,
                              16, // 48 / 16 = 3 chunks
                              1); // 3 / 1 = 3 shards
            dims.emplace_back("z",
                              DimensionType_Space,
                              5,
                              2,  // 3 chunks, ragged
                              1); // 3 / 1 = 3 shards
            dims.emplace_back("t",
                              DimensionType_Time,
                              0,
                              5,  // 5 timepoints / chunk
                              2); // 2 chunks / shard

            zarr::WriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3Writer writer(config, thread_pool);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48);
            frame->bytes_of_frame =
              common::align_up(sizeof(VideoFrame) + 64 * 48, 8);
            frame->shape = shape;
            memset(frame->data, 0, 64 * 48);

            for (auto i = 0; i < 5 * 10; ++i) { // 10 time points (2 chunks)
                frame->frame_id = i;
                CHECK(writer.write(frame));
            }
            writer.finalize();

            const auto chunk_size = 16 *               // x
                                    16 *               // y
                                    2 *                // z
                                    5;                 // t
            const auto index_size = 4 *                // 4 chunks
                                    sizeof(uint64_t) * // indices are 64 bits
                                    2;                 // 2 indices per chunk
            const auto expected_file_size = 2 *        // x
                                              1 *      // y
                                              1 *      // z
                                              2 *      // t
                                              chunk_size +
                                            index_size;

            CHECK(fs::is_directory(base_dir));
            for (auto t = 0; t < 1; ++t) {
                const auto t_dir = base_dir / ("c" + std::to_string(t));
                CHECK(fs::is_directory(t_dir));

                for (auto z = 0; z < 3; ++z) {
                    const auto z_dir = t_dir / std::to_string(z);
                    CHECK(fs::is_directory(z_dir));

                    for (auto y = 0; y < 3; ++y) {
                        const auto y_dir = z_dir / std::to_string(y);
                        CHECK(fs::is_directory(y_dir));

                        for (auto x = 0; x < 2; ++x) {
                            const auto x_file = y_dir / std::to_string(x);
                            CHECK(fs::is_regular_file(x_file));
                            const auto file_size = fs::file_size(x_file);
                            CHECK(file_size == expected_file_size);
                        }

                        CHECK(!fs::is_regular_file(y_dir / "2"));
                    }

                    CHECK(!fs::is_directory(z_dir / "3"));
                }

                CHECK(!fs::is_directory(t_dir / "3"));
            }

            CHECK(!fs::is_directory(base_dir / "c1"));

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        // cleanup
        if (fs::exists(base_dir)) {
            fs::remove_all(base_dir);
        }
        if (frame) {
            free(frame);
        }
        return retval;
    }
}
#endif
