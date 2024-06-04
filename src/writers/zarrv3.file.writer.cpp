#include "zarrv3.file.writer.hh"
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
                 std::shared_ptr<ThreadPool>& thread_pool)
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
create_shard_files(const std::string& data_root,
                   std::vector<zarr::Dimension>& dimensions,
                   std::shared_ptr<ThreadPool>& thread_pool,
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
        const auto n_shards = common::shards_along_dimension(dim);

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_shards; ++k) {
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
        const auto n_shards = common::shards_along_dimension(dim);

        auto n_paths = paths.size();
        for (auto i = 0; i < n_paths; ++i) {
            const auto path = paths.front();
            paths.pop();
            for (auto j = 0; j < n_shards; ++j) {
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

zarr::ZarrV3FileWriter::ZarrV3FileWriter(
  const WriterConfig& config,
  std::shared_ptr<ThreadPool> thread_pool)
  : FileWriter(config, thread_pool)
  , shard_file_offsets_(common::number_of_shards(config.dimensions), 0)
  , shard_tables_{ common::number_of_shards(config.dimensions) }
{
    const auto chunks_per_shard = common::chunks_per_shard(config.dimensions);

    for (auto& table : shard_tables_) {
        table.resize(2 * chunks_per_shard);
        std::fill_n(
          table.begin(), table.size(), std::numeric_limits<uint64_t>::max());
    }

    // precompute the number of frames to acquire before rolling over
    frames_per_shard_ = config.dimensions.back().chunk_size_px *
                        config.dimensions.back().shard_size_chunks;
    for (auto i = 2; i < config.dimensions.size() - 1; ++i) {
        frames_per_shard_ *= config.dimensions.at(i).array_size_px;
    }
    EXPECT(frames_per_shard_ > 0, "A dimension has a size of 0.");
}

bool
zarr::ZarrV3FileWriter::flush_impl_()
{
    // create files if they don't exist
    const std::string data_root = (fs::path(writer_config_.data_root) /
                                   ("c" + std::to_string(append_chunk_index_)))
                                    .string();

    if (files_.empty() &&
        !create_shard_files(
          data_root, writer_config_.dimensions, thread_pool_, files_)) {
        return false;
    }

    const auto n_shards = common::number_of_shards(writer_config_.dimensions);
    CHECK(files_.size() == n_shards);

    // get shard indices for each chunk
    std::vector<std::vector<size_t>> chunk_in_shards(n_shards);
    for (auto i = 0; i < chunk_buffers_.size(); ++i) {
        const auto index =
          common::shard_index_for_chunk(i, writer_config_.dimensions);
        chunk_in_shards.at(index).push_back(i);
    }

    bool write_table = is_finalizing_ || should_rollover_();

    // write out chunks to shards
    std::latch latch(n_shards);
    {
        for (auto i = 0; i < n_shards; ++i) {
            const auto& chunks = chunk_in_shards.at(i);
            auto& chunk_table = shard_tables_.at(i);
            size_t* file_offset = &shard_file_offsets_.at(i);

            thread_pool_->push_to_job_queue(
              std::move([file = files_.at(i).get(),
                         &chunks,
                         &chunk_table,
                         file_offset,
                         write_table,
                         &latch,
                         this](std::string& err) mutable -> bool {
                  bool success = false;
                  try {
                      for (const auto& chunk_index : chunks) {
                          auto& chunk = chunk_buffers_.at(chunk_index);

                          const uint8_t* data = chunk.data();
                          const size_t size = chunk.size();
                          if (!(success = (bool)file_write(
                                  file, *file_offset, data, data + size))) {
                              break;
                          }

                          // update the chunk (offset, extent) for this shard
                          const auto internal_index =
                            common::shard_internal_index(
                              chunk_index, writer_config_.dimensions);
                          chunk_table.at(2 * internal_index) = *file_offset;
                          chunk_table.at(2 * internal_index + 1) = chunk.size();

                          // update the offset within the file
                          *file_offset += chunk.size();

                          if (write_table) {
                              const auto* table =
                                reinterpret_cast<const uint8_t*>(
                                  chunk_table.data());
                              const auto table_size =
                                chunk_table.size() * sizeof(uint64_t);

                              if (!(success =
                                      (bool)file_write(file,
                                                       *file_offset,
                                                       data,
                                                       data + table_size))) {
                                  break;
                              }
                          }
                      }
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

    return false;
}

bool
zarr::ZarrV3FileWriter::should_rollover_() const
{
    return frames_written_ % frames_per_shard_ == 0;
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
            auto thread_pool = std::make_shared<ThreadPool>(
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
                .type = SampleType_u16,
            };

            zarr::WriterConfig writer_config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3FileWriter writer(writer_config, thread_pool);

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
            auto thread_pool = std::make_shared<ThreadPool>(
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
                .type = SampleType_u8,
            };

            zarr::WriterConfig writer_config = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3FileWriter writer(writer_config, thread_pool);

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
            auto thread_pool = std::make_shared<ThreadPool>(
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

            zarr::ZarrV3FileWriter writer(writer_config, thread_pool);

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