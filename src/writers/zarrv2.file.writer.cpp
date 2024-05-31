#include "zarrv2.file.writer.hh"
#include "platform.h"

#include <filesystem>
#include <latch>
#include <queue>

namespace fs = std::filesystem;
namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

namespace {
bool
make_directories(std::queue<fs::path>& dir_paths,
                 std::shared_ptr<common::ThreadPool>& thread_pool)
{
    if (dir_paths.empty()) {
        return true;
    }

    std::atomic<bool> all_successful = true;

    const auto n_dirs = dir_paths.size();
    std::latch latch(n_dirs);

    for (auto i = 0; i < n_dirs; ++i) {
        const auto dirname = dir_paths.front();
        dir_paths.pop();

        thread_pool->push_to_job_queue(
          [dirname, &latch, &all_successful](std::string& err) -> bool {
              bool success = false;

              try {
                  if (fs::exists(dirname)) {
                      EXPECT(fs::is_directory(dirname),
                             "'%s' exists but is not a directory",
                             dirname.c_str());
                  } else if (all_successful) {
                      std::error_code ec;
                      EXPECT(fs::create_directories(dirname, ec),
                             "%s",
                             ec.message().c_str());
                  }
                  success = true;
              } catch (const std::exception& exc) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create directory '%s': %s.",
                           dirname.string().c_str(),
                           exc.what());
                  err = buf;
              } catch (...) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create directory '%s': (unknown).",
                           dirname.string().c_str());
                  err = buf;
              }

              latch.count_down();
              all_successful = all_successful && success;

              return success;
          });

        dir_paths.push(dirname);
    }

    latch.wait();

    return all_successful;
}

bool
create_chunk_files(const std::string& data_root,
                   std::vector<zarr::Dimension>& dimensions,
                   std::shared_ptr<common::ThreadPool>& thread_pool,
                   std::vector<std::unique_ptr<struct file>>& files)
{
    std::queue<fs::path> paths;
    paths.emplace(data_root);

    if (!make_directories(paths, thread_pool)) {
        return false;
    }

    // create directories
    for (auto i = dimensions.size() - 2; i >= 1; --i) {
        const auto& dim = dimensions.at(i);
        const auto n_chunks = common::chunks_along_dimension(dim);

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_chunks; ++k) {
                paths.push(path / std::to_string(k));
            }
        }

        if (!make_directories(paths, thread_pool)) {
            return false;
        }
    }

    // create files
    {
        const auto& dim = dimensions.front();
        const auto n_chunks = common::chunks_along_dimension(dim);

        auto n_paths = paths.size();
        for (auto i = 0; i < n_paths; ++i) {
            const auto path = paths.front();
            paths.pop();
            for (auto j = 0; j < n_chunks; ++j) {
                paths.push(path / std::to_string(j));
            }
        }
    }

    std::atomic<bool> all_successful = true;

    const auto n_files = paths.size();
    files.resize(n_files);

    std::fill(files.begin(), files.end(), nullptr);
    std::latch latch(n_files);

    for (auto i = 0; i < n_files; ++i) {
        const auto filename = paths.front().string();
        paths.pop();

        std::unique_ptr<struct file>* pfile = files.data() + i;

        thread_pool->push_to_job_queue(
          [filename, pfile, &latch, &all_successful](std::string& err) -> bool {
              bool success = false;

              try {
                  if (all_successful) {
                      *pfile = std::make_unique<struct file>();
                      CHECK(*pfile != nullptr);
                      CHECK(file_create(
                        pfile->get(), filename.c_str(), filename.size()));
                  }
                  success = true;
              } catch (const std::exception& exc) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create file '%s': %s.",
                           filename.c_str(),
                           exc.what());
                  err = buf;
              } catch (...) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create file '%s': (unknown).",
                           filename.c_str());
                  err = buf;
              }

              latch.count_down();
              all_successful = all_successful && success;

              return success;
          });
    }

    latch.wait();

    return all_successful;
}
} // namespace

zarr::ZarrV2FileWriter::ZarrV2FileWriter(
  const WriterConfig& config,
  std::shared_ptr<common::ThreadPool> thread_pool)
  : ZarrV2Writer(config, thread_pool)
  , FileWriter()
{
}

bool
zarr::ZarrV2FileWriter::flush_impl_()
{
    try {
        CHECK(files_.empty());

        CHECK(create_chunk_files(writer_config_.data_root,
                                 writer_config_.dimensions,
                                 thread_pool_,
                                 files_));
        CHECK(files_.size() == chunk_buffers_.size());
    } catch (const std::exception& exc) {
        LOGE("Failed to create sinks: %s", exc.what());
        return false;
    } catch (...) {
        LOGE("Failed to create sinks: (unknown)");
        return false;
    }

    std::latch latch(files_.size());
    {
        std::scoped_lock lock(buffers_mutex_);
        for (auto i = 0; i < files_.size(); ++i) {
            auto& chunk = chunk_buffers_.at(i);
            thread_pool_->push_to_job_queue(
              std::move([&file = files_.at(i),
                         data = chunk.data(),
                         size = chunk.size(),
                         &latch](std::string& err) -> bool {
                  bool success = false;
                  try {
                      CHECK(file_write(file.get(), 0, data, data + size));
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

    latch.wait();

    return true;
}

bool
zarr::ZarrV2FileWriter::should_rollover_() const
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

            zarr::WriterConfig writer_config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV2FileWriter writer(writer_config, thread_pool);

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

            zarr::WriterConfig writer_config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV2FileWriter writer(writer_config, thread_pool);

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

            zarr::WriterConfig writer_config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV2FileWriter writer(writer_config, thread_pool);

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