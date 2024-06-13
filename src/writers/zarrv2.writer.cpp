#include "zarrv2.writer.hh"
#include "../zarr.hh"

#include <cmath>
#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ZarrV2Writer::ZarrV2Writer(
  const WriterConfig& config,
  std::shared_ptr<common::ThreadPool> thread_pool)
  : Writer(config, thread_pool)
{
}

bool
zarr::ZarrV2Writer::flush_impl_()
{
    // create chunk files
    CHECK(sinks_.empty());
    const std::string data_root =
      (fs::path(data_root_) / std::to_string(append_chunk_index_)).string();

    {
        FileCreator file_creator(thread_pool_);
        if (!file_creator.create_chunk_sinks(
              data_root, config_.dimensions, sinks_)) {
            return false;
        }
    }

    CHECK(sinks_.size() == chunk_buffers_.size());

    std::latch latch(chunk_buffers_.size());
    {
        std::scoped_lock lock(buffers_mutex_);
        for (auto i = 0; i < sinks_.size(); ++i) {
            auto& chunk = chunk_buffers_.at(i);
            thread_pool_->push_to_job_queue(
              std::move([sink = sinks_.at(i),
                         data = chunk.data(),
                         size = chunk.size(),
                         &latch](std::string& err) -> bool {
                  bool success = false;
                  try {
                      CHECK(sink->write(0, data, size));
                      success = true;
                  } catch (const std::exception& exc) {
                      char buf[128];
                      snprintf(buf,
                               sizeof(buf),
                               "Failed to write chunk: %s",
                               exc.what());
                      err = buf;
                  } catch (...) {
                      err = "Unknown error";
                  }

                  latch.count_down();
                  return success;
              }));
        }
    }

    // wait for all threads to finish
    latch.wait();

    return true;
}

bool
zarr::ZarrV2Writer::should_rollover_() const
{
    return true;
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
    acquire_export int unit_test__zarrv2_writer__write_even()
    {
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        struct VideoFrame* frame = nullptr;
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s\n", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x", DimensionType_Space, 64, 16, 0); // 4 chunks
            dims.emplace_back("y", DimensionType_Space, 48, 16, 0); // 3 chunks
            dims.emplace_back("z", DimensionType_Space, 6, 2, 0);   // 3 chunks
            dims.emplace_back("c", DimensionType_Channel, 8, 4, 0); // 2 chunks
            dims.emplace_back(
              "t", DimensionType_Time, 0, 5, 0); // 5 timepoints / chunk

            ImageShape shape {
                .dims = {
                  .width = 64,
                  .height = 48,
                },
                .type = SampleType_u16,
            };

            zarr::WriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV2Writer writer(config, thread_pool);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48 * 2);
            frame->bytes_of_frame = sizeof(VideoFrame) + 64 * 48 * 2;
            frame->shape = shape;
            memset(frame->data, 0, 64 * 48 * 2);

            for (auto i = 0; i < 6 * 8 * 5 * 2; ++i) { // 2 time points
                frame->frame_id = i;
                CHECK(writer.write(frame));
            }
            writer.finalize();

            const auto expected_file_size = 16 * // x
                                            16 * // y
                                            2 *  // z
                                            4 *  // c
                                            5 *  // t
                                            2;   // bytes per pixel

            CHECK(fs::is_directory(base_dir));
            for (auto t = 0; t < 2; ++t) {
                const auto t_dir = base_dir / std::to_string(t);
                CHECK(fs::is_directory(t_dir));

                for (auto c = 0; c < 2; ++c) {
                    const auto c_dir = t_dir / std::to_string(c);
                    CHECK(fs::is_directory(c_dir));

                    for (auto z = 0; z < 3; ++z) {
                        const auto z_dir = c_dir / std::to_string(z);
                        CHECK(fs::is_directory(z_dir));

                        for (auto y = 0; y < 3; ++y) {
                            const auto y_dir = z_dir / std::to_string(y);
                            CHECK(fs::is_directory(y_dir));

                            for (auto x = 0; x < 4; ++x) {
                                const auto x_file = y_dir / std::to_string(x);
                                CHECK(fs::is_regular_file(x_file));
                                const auto file_size = fs::file_size(x_file);
                                CHECK(file_size == expected_file_size);
                            }

                            CHECK(!fs::is_regular_file(y_dir / "4"));
                        }

                        CHECK(!fs::is_directory(z_dir / "3"));
                    }

                    CHECK(!fs::is_directory(c_dir / "3"));
                }

                CHECK(!fs::is_directory(t_dir / "2"));
            }

            CHECK(!fs::is_directory(base_dir / "2"));

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

    acquire_export int unit_test__zarrv2_writer__write_ragged_append_dim()
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
                .type = SampleType_u8,
            };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x", DimensionType_Space, 64, 16, 0); // 4 chunks
            dims.emplace_back("y", DimensionType_Space, 48, 16, 0); // 3 chunks
            dims.emplace_back(
              "z", DimensionType_Space, 5, 2, 0); // 3 chunks, ragged

            zarr::WriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV2Writer writer(config, thread_pool);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48);
            frame->bytes_of_frame = sizeof(VideoFrame) + 64 * 48;
            frame->shape = shape;
            memset(frame->data, 0, 64 * 48);

            for (auto i = 0; i < 5; ++i) { // z dimension is ragged
                frame->frame_id = i;
                CHECK(writer.write(frame));
            }
            writer.finalize();

            const auto expected_file_size = 16 * // x
                                            16 * // y
                                            2;   // z

            CHECK(fs::is_directory(base_dir));
            for (auto z = 0; z < 3; ++z) {
                const auto z_dir = base_dir / std::to_string(z);
                CHECK(fs::is_directory(z_dir));

                for (auto y = 0; y < 3; ++y) {
                    const auto y_dir = z_dir / std::to_string(y);
                    CHECK(fs::is_directory(y_dir));

                    for (auto x = 0; x < 4; ++x) {
                        const auto x_file = y_dir / std::to_string(x);
                        CHECK(fs::is_regular_file(x_file));
                        const auto file_size = fs::file_size(x_file);
                        CHECK(file_size == expected_file_size);
                    }

                    CHECK(!fs::is_regular_file(y_dir / "4"));
                }

                CHECK(!fs::is_directory(z_dir / "3"));
            }

            CHECK(!fs::is_directory(base_dir / "3"));

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

    acquire_export int unit_test__zarrv2_writer__write_ragged_internal_dim()
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
                .type = SampleType_u8,
            };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x", DimensionType_Space, 64, 16, 0); // 4 chunks
            dims.emplace_back("y", DimensionType_Space, 48, 16, 0); // 3 chunks
            dims.emplace_back(
              "z", DimensionType_Space, 5, 2, 0); // 3 chunks, ragged
            dims.emplace_back(
              "t", DimensionType_Time, 0, 5, 0); // 5 timepoints / chunk

            zarr::WriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV2Writer writer(config, thread_pool);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48);
            frame->bytes_of_frame = sizeof(VideoFrame) + 64 * 48;
            frame->shape = shape;
            memset(frame->data, 0, 64 * 48);

            for (auto i = 0; i < 2 * 5; ++i) { // 5 time points
                frame->frame_id = i;
                CHECK(writer.write(frame));
            }
            writer.finalize();

            const auto expected_file_size = 16 * // x
                                            16 * // y
                                            2 *  // z
                                            5;   // t

            CHECK(fs::is_directory(base_dir));
            for (auto t = 0; t < 1; ++t) {
                const auto t_dir = base_dir / std::to_string(t);
                CHECK(fs::is_directory(t_dir));

                for (auto z = 0; z < 3; ++z) {
                    const auto z_dir = t_dir / std::to_string(z);
                    CHECK(fs::is_directory(z_dir));

                    for (auto y = 0; y < 3; ++y) {
                        const auto y_dir = z_dir / std::to_string(y);
                        CHECK(fs::is_directory(y_dir));

                        for (auto x = 0; x < 4; ++x) {
                            const auto x_file =
                              base_dir / std::to_string(t) / std::to_string(z) /
                              std::to_string(y) / std::to_string(x);
                            CHECK(fs::is_regular_file(x_file));
                            const auto file_size = fs::file_size(x_file);
                            CHECK(file_size == expected_file_size);
                        }

                        CHECK(!fs::is_regular_file(y_dir / "4"));
                    }

                    CHECK(!fs::is_directory(z_dir / "3"));
                }

                CHECK(!fs::is_directory(t_dir / "3"));
            }
            CHECK(!fs::is_directory(base_dir / "1"));

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
