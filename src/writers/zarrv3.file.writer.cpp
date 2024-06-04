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
