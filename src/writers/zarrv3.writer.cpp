#include "zarrv3.writer.hh"
#include "sink.creator.hh"
#include "zarr.hh"

#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ZarrV3Writer::ZarrV3Writer(
  const WriterConfig& array_spec,
  std::shared_ptr<common::ThreadPool> thread_pool,
  std::shared_ptr<common::S3ConnectionPool> connection_pool)
  : Writer(array_spec, thread_pool, connection_pool)
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
        SinkCreator creator(thread_pool_, connection_pool_);
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
        const auto index = common::shard_index_for_chunk(i, config_.dimensions);
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

                    const auto internal_idx = common::shard_internal_index(
                      chunk_idx, config_.dimensions);
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
    acquire_export int unit_test__zarrv3_writer__write_even()
    {
        int retval = 0;
        const fs::path base_dir = fs::temp_directory_path() / "acquire";

        const unsigned int array_width = 64, array_height = 48,
                           array_planes = 6, array_channels = 8,
                           array_timepoints = 10;
        const unsigned int n_frames =
          array_planes * array_channels * array_timepoints;

        const unsigned int chunk_width = 16, chunk_height = 16,
                           chunk_planes = 2, chunk_channels = 4,
                           chunk_timepoints = 5;

        const unsigned int shard_width = 2, shard_height = 1, shard_planes = 1,
                           shard_channels = 2, shard_timepoints = 2;
        const unsigned int chunks_per_shard = shard_width * shard_height *
                                              shard_planes * shard_channels *
                                              shard_timepoints;

        const unsigned int chunks_in_x =
          (array_width + chunk_width - 1) / chunk_width; // 4 chunks
        const unsigned int chunks_in_y =
          (array_height + chunk_height - 1) / chunk_height; // 3 chunks
        const unsigned int chunks_in_z =
          (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks
        const unsigned int chunks_in_c =
          (array_channels + chunk_channels - 1) / chunk_channels; // 2 chunks
        const unsigned int chunks_in_t =
          (array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

        const unsigned int shards_in_x =
          (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
        const unsigned int shards_in_y =
          (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
        const unsigned int shards_in_z =
          (chunks_in_z + shard_planes - 1) / shard_planes; // 3 shards
        const unsigned int shards_in_c =
          (chunks_in_c + shard_channels - 1) / shard_channels; // 1 shard
        const unsigned int shards_in_t =
          (chunks_in_t + shard_timepoints - 1) / shard_timepoints; // 1 shard

        const ImageShape shape
          {
              .dims = {
                .width = array_width,
                .height = array_height,
              },
              .strides = {
                .width = 1,
                .height = array_width,
                .planes = array_width * array_height,
              },
              .type = SampleType_u16,
          };
        const unsigned int nbytes_px = bytes_of_type(shape.type);

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, array_width, chunk_width, shard_width);
            dims.emplace_back("y",
                              DimensionType_Space,
                              array_height,
                              chunk_height,
                              shard_height);
            dims.emplace_back("z",
                              DimensionType_Space,
                              array_planes,
                              chunk_planes,
                              shard_planes);
            dims.emplace_back("c",
                              DimensionType_Channel,
                              array_channels,
                              chunk_channels,
                              shard_channels);
            dims.emplace_back("t",
                              DimensionType_Time,
                              array_timepoints,
                              chunk_timepoints,
                              shard_timepoints);

            zarr::WriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3Writer writer(
              config, thread_pool, std::shared_ptr<common::S3ConnectionPool>());

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector<uint8_t> data(frame_size, 0);

            for (auto i = 0; i < n_frames; ++i) {
                CHECK(writer.write(data.data(), frame_size));
            }
            writer.finalize();

            const auto chunk_size = chunk_width * chunk_height * chunk_planes *
                                    chunk_channels * chunk_timepoints *
                                    nbytes_px;
            const auto index_size = chunks_per_shard *
                                    sizeof(uint64_t) * // indices are 64 bits
                                    2;                 // 2 indices per chunk
            const auto expected_file_size = shard_width * shard_height *
                                              shard_planes * shard_channels *
                                              shard_timepoints * chunk_size +
                                            index_size;

            CHECK(fs::is_directory(base_dir));
            for (auto t = 0; t < shards_in_t; ++t) {
                const auto t_dir = base_dir / ("c" + std::to_string(t));
                CHECK(fs::is_directory(t_dir));

                for (auto c = 0; c < shards_in_c; ++c) {
                    const auto c_dir = t_dir / std::to_string(c);
                    CHECK(fs::is_directory(c_dir));

                    for (auto z = 0; z < shards_in_z; ++z) {
                        const auto z_dir = c_dir / std::to_string(z);
                        CHECK(fs::is_directory(z_dir));

                        for (auto y = 0; y < shards_in_y; ++y) {
                            const auto y_dir = z_dir / std::to_string(y);
                            CHECK(fs::is_directory(y_dir));

                            for (auto x = 0; x < shards_in_x; ++x) {
                                const auto x_file = y_dir / std::to_string(x);
                                CHECK(fs::is_regular_file(x_file));
                                const auto file_size = fs::file_size(x_file);
                                CHECK(file_size == expected_file_size);
                            }

                            CHECK(!fs::is_regular_file(
                              y_dir / std::to_string(shards_in_x)));
                        }

                        CHECK(!fs::is_directory(z_dir /
                                                std::to_string(shards_in_y)));
                    }

                    CHECK(
                      !fs::is_directory(c_dir / std::to_string(shards_in_z)));
                }

                CHECK(!fs::is_directory(t_dir / std::to_string(shards_in_c)));
            }

            CHECK(!fs::is_directory(base_dir /
                                    ("c" + std::to_string(shards_in_t))));

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
        return retval;
    }

    acquire_export int unit_test__zarrv3_writer__write_ragged_append_dim()
    {
        int retval = 0;
        const fs::path base_dir = fs::temp_directory_path() / "acquire";

        const unsigned int array_width = 64, array_height = 48,
                           array_planes = 5;
        const unsigned int n_frames = array_planes;

        const unsigned int chunk_width = 16, chunk_height = 16,
                           chunk_planes = 2;

        const unsigned int shard_width = 2, shard_height = 1, shard_planes = 1;
        const unsigned int chunks_per_shard =
          shard_width * shard_height * shard_planes;

        const unsigned int chunks_in_x =
          (array_width + chunk_width - 1) / chunk_width; // 4 chunks
        const unsigned int chunks_in_y =
          (array_height + chunk_height - 1) / chunk_height; // 3 chunks
        const unsigned int chunks_in_z =
          (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks

        const unsigned int shards_in_x =
          (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
        const unsigned int shards_in_y =
          (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
        const unsigned int shards_in_z =
          (chunks_in_z + shard_planes - 1) / shard_planes; // 3 shards

        const ImageShape shape
          {
              .dims = {
                .width = array_width,
                .height = array_height,
              },
              .strides = {
                .width = 1,
                .height = array_width,
                .planes = array_width * array_height,
              },
              .type = SampleType_i8,
          };
        const unsigned int nbytes_px = bytes_of_type(shape.type);

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, array_width, chunk_width, shard_width);
            dims.emplace_back("y",
                              DimensionType_Space,
                              array_height,
                              chunk_height,
                              shard_height);
            dims.emplace_back("z",
                              DimensionType_Space,
                              array_planes,
                              chunk_planes,
                              shard_planes);

            zarr::WriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3Writer writer(
              config, thread_pool, std::shared_ptr<common::S3ConnectionPool>());

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector<uint8_t> data(frame_size, 0);

            for (auto i = 0; i < n_frames; ++i) {
                CHECK(writer.write(data.data(), frame_size) == frame_size);
            }
            writer.finalize();

            const auto chunk_size =
              chunk_width * chunk_height * chunk_planes * nbytes_px;
            const auto index_size = chunks_per_shard *
                                    sizeof(uint64_t) * // indices are 64 bits
                                    2;                 // 2 indices per chunk
            const auto expected_file_size =
              shard_width * shard_height * shard_planes * chunk_size +
              index_size;

            CHECK(fs::is_directory(base_dir));
            for (auto z = 0; z < shards_in_z; ++z) {
                const auto z_dir = base_dir / ("c" + std::to_string(z));
                CHECK(fs::is_directory(z_dir));

                for (auto y = 0; y < shards_in_y; ++y) {
                    const auto y_dir = z_dir / std::to_string(y);
                    CHECK(fs::is_directory(y_dir));

                    for (auto x = 0; x < shards_in_x; ++x) {
                        const auto x_file = y_dir / std::to_string(x);
                        CHECK(fs::is_regular_file(x_file));
                        const auto file_size = fs::file_size(x_file);
                        CHECK(file_size == expected_file_size);
                    }

                    CHECK(!fs::is_regular_file(y_dir /
                                               std::to_string(shards_in_x)));
                }

                CHECK(!fs::is_directory(z_dir / std::to_string(shards_in_y)));
            }

            CHECK(!fs::is_directory(base_dir /
                                    ("c" + std::to_string(shards_in_z))));

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
        return retval;
    }

    acquire_export int unit_test__zarrv3_writer__write_ragged_internal_dim()
    {
        int retval = 0;
        const fs::path base_dir = fs::temp_directory_path() / "acquire";

        const unsigned int array_width = 64, array_height = 48,
                           array_planes = 5, array_timepoints = 10;
        const unsigned int n_frames = array_planes * array_timepoints;

        const unsigned int chunk_width = 16, chunk_height = 16,
                           chunk_planes = 2, chunk_timepoints = 5;

        const unsigned int shard_width = 2, shard_height = 1, shard_planes = 1,
                           shard_timepoints = 2;
        const unsigned int chunks_per_shard =
          shard_width * shard_height * shard_planes * shard_timepoints;

        const unsigned int chunks_in_x =
          (array_width + chunk_width - 1) / chunk_width; // 4 chunks
        const unsigned int chunks_in_y =
          (array_height + chunk_height - 1) / chunk_height; // 3 chunks
        const unsigned int chunks_in_z =
          (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks, ragged
        const unsigned int chunks_in_t =
          (array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

        const unsigned int shards_in_x =
          (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
        const unsigned int shards_in_y =
          (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
        const unsigned int shards_in_z =
          (chunks_in_z + shard_planes - 1) / shard_planes; // 3 shards
        const unsigned int shards_in_t =
          (chunks_in_t + shard_timepoints - 1) / shard_timepoints; // 1 shard

        const ImageShape shape
          {
              .dims = {
                .width = array_width,
                .height = array_height,
              },
              .strides = {
                .width = 1,
                .height = array_width,
                .planes = array_width * array_height,
              },
              .type = SampleType_f32,
          };
        const unsigned int nbytes_px = bytes_of_type(shape.type);

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, array_width, chunk_width, shard_width);
            dims.emplace_back("y",
                              DimensionType_Space,
                              array_height,
                              chunk_height,
                              shard_height);
            dims.emplace_back("z",
                              DimensionType_Space,
                              array_planes,
                              chunk_planes,
                              shard_planes);
            dims.emplace_back("t",
                              DimensionType_Time,
                              array_timepoints,
                              chunk_timepoints,
                              shard_timepoints);

            zarr::WriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3Writer writer(
              config, thread_pool, std::shared_ptr<common::S3ConnectionPool>());

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector<uint8_t> data(frame_size, 0);

            for (auto i = 0; i < n_frames; ++i) {
                CHECK(writer.write(data.data(), frame_size) == frame_size);
            }
            writer.finalize();

            const auto chunk_size = chunk_width * chunk_height * chunk_planes *
                                    chunk_timepoints * nbytes_px;
            const auto index_size = chunks_per_shard *
                                    sizeof(uint64_t) * // indices are 64 bits
                                    2;                 // 2 indices per chunk
            const auto expected_file_size = shard_width * shard_height *
                                              shard_planes * shard_timepoints *
                                              chunk_size +
                                            index_size;

            CHECK(fs::is_directory(base_dir));
            for (auto t = 0; t < shards_in_t; ++t) {
                const auto t_dir = base_dir / ("c" + std::to_string(t));
                CHECK(fs::is_directory(t_dir));

                for (auto z = 0; z < shards_in_z; ++z) {
                    const auto z_dir = t_dir / std::to_string(z);
                    CHECK(fs::is_directory(z_dir));

                    for (auto y = 0; y < shards_in_y; ++y) {
                        const auto y_dir = z_dir / std::to_string(y);
                        CHECK(fs::is_directory(y_dir));

                        for (auto x = 0; x < shards_in_x; ++x) {
                            const auto x_file = y_dir / std::to_string(x);
                            CHECK(fs::is_regular_file(x_file));
                            const auto file_size = fs::file_size(x_file);
                            CHECK(file_size == expected_file_size);
                        }

                        CHECK(!fs::is_regular_file(
                          y_dir / std::to_string(shards_in_x)));
                    }

                    CHECK(
                      !fs::is_directory(z_dir / std::to_string(shards_in_y)));
                }

                CHECK(!fs::is_directory(t_dir / std::to_string(shards_in_z)));
            }

            CHECK(!fs::is_directory(base_dir /
                                    ("c" + std::to_string(shards_in_t))));

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
        return retval;
    }
}
#endif
