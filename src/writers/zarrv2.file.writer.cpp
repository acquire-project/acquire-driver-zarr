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
create_chunk_files(const std::string& data_root,
                   std::vector<zarr::Dimension>& dimensions,
                   std::shared_ptr<ThreadPool>& thread_pool,
                   std::vector<std::unique_ptr<struct file>>& files)
{


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
  std::shared_ptr<ThreadPool> thread_pool)
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