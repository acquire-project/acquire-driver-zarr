#include "zarrv3.writer.hh"

#include "../zarr.hh"
#include "sink.creator.hh"

#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

namespace {
bool
is_s3_uri(const std::string& uri)
{
    return uri.starts_with("s3://") || uri.starts_with("http://") ||
           uri.starts_with("https://");
}
} // namespace

zarr::ZarrV3Writer::ZarrV3Writer(
  const WriterConfig& array_spec,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<S3ConnectionPool> connection_pool)
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
    const std::string data_root = writer_config_.data_root + "/" +
                                  ("c" + std::to_string(append_chunk_index_));

    if (sinks_.empty()) {
        SinkCreator creator{ thread_pool_, connection_pool_ };
        if (!creator.create_shard_sinks(
              data_root, writer_config_.dimensions, sinks_)) {
            return false;
        }
    }

    //    // create shard sinks if they don't exist
    //    if (is_s3_uri(data_root_)) {
    //        std::vector<std::string> uri_parts =
    //        common::split_uri(data_root_); CHECK(uri_parts.size() > 2); //
    //        s3://bucket/key std::string endpoint = uri_parts.at(0) + "//" +
    //        uri_parts.at(1); std::string bucket_name = uri_parts.at(2);
    //
    //        std::string data_root;
    //        for (auto i = 3; i < uri_parts.size() - 1; ++i) {
    //            data_root += uri_parts.at(i) + "/";
    //        }
    //        if (uri_parts.size() > 2) {
    //            data_root += uri_parts.back();
    //        }
    //
    //        S3SinkCreator creator{ thread_pool_,
    //                               endpoint,
    //                               bucket_name,
    //                               array_config_.access_key_id,
    //                               array_config_.secret_access_key };
    //
    //        size_t min_chunk_size_bytes = 0;
    //        for (const auto& buf : chunk_buffers_) {
    //            min_chunk_size_bytes = std::max(min_chunk_size_bytes,
    //            buf.size());
    //        }
    //
    //        CHECK(creator.create_shard_sinks(
    //          data_root, array_config_.dimensions, sinks_,
    //          min_chunk_size_bytes));
    //    } else {
    //        const std::string data_root =
    //          (fs::path(data_root_) / ("c" +
    //          std::to_string(append_chunk_index_)))
    //            .string();
    //
    //        FileCreator file_creator(thread_pool_);
    //        if (sinks_.empty() && !file_creator.create_shard_sinks(
    //                                data_root, array_config_.dimensions,
    //                                sinks_)) {
    //            return false;
    //        }
    //    }

    const auto n_shards = common::number_of_shards(writer_config_.dimensions);
    CHECK(sinks_.size() == n_shards);

    // get shard indices for each chunk
    std::vector<std::vector<size_t>> chunk_in_shards(n_shards);
    for (auto i = 0; i < chunk_buffers_.size(); ++i) {
        const auto index =
          common::shard_index_for_chunk(i, writer_config_.dimensions);
        chunk_in_shards.at(index).push_back(i);
    }

    // write out chunks to shards
    bool write_table = is_finalizing_ || should_rollover_();
    std::latch latch(n_shards);
    for (auto i = 0; i < n_shards; ++i) {
        const auto& chunks = chunk_in_shards.at(i);
        auto& chunk_table = shard_tables_.at(i);
        size_t* file_offset = &shard_file_offsets_.at(i);

        thread_pool_->push_to_job_queue([sink = sinks_.at(i),
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
                      chunk_idx, writer_config_.dimensions);
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
                char buf[128];
                snprintf(
                  buf, sizeof(buf), "Failed to write chunk: %s", exc.what());
                err = buf;
            } catch (...) {
                err = "Unknown error";
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
    const auto& dims = writer_config_.dimensions;
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
        struct VideoFrame* frame = nullptr;

        try {
            auto thread_pool = std::make_shared<zarr::ThreadPool>(
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
                  .width = 1,
                  .height = 64,
                  .planes = 64 * 48,
                },
                .type = SampleType_u16,
            };

            zarr::WriterConfig writer_config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3Writer writer(writer_config, thread_pool, nullptr);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48 * 2);
            frame->bytes_of_frame = sizeof(VideoFrame) + 64 * 48 * 2;
            frame->shape = shape;
            memset(frame->data, 0, 64 * 48 * 2);

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
            auto thread_pool = std::make_shared<zarr::ThreadPool>(
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
                  .width = 1,
                  .height = 64,
                  .planes = 64 * 48,
                },
                .type = SampleType_u8,
            };

            zarr::WriterConfig writer_config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3Writer writer(writer_config, thread_pool, nullptr);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48);
            frame->bytes_of_frame = sizeof(VideoFrame) + 64 * 48;
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
            auto thread_pool = std::make_shared<zarr::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            ImageShape shape {
                .dims = {
                  .width = 64,
                  .height = 48,
                },
                .strides = {
                  .width = 1,
                  .height = 64,
                  .planes = 64 * 48,
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

            zarr::WriterConfig writer_config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3Writer writer(writer_config, thread_pool, nullptr);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48);
            frame->bytes_of_frame = sizeof(VideoFrame) + 64 * 48;
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