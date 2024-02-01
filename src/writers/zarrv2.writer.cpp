#include "zarrv2.writer.hh"
#include "../zarr.hh"

#include <cmath>
#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ZarrV2Writer::ZarrV2Writer(
  const ArraySpec& array_spec,
  std::shared_ptr<common::ThreadPool> thread_pool)
  : Writer(array_spec, thread_pool)
{
}

void
zarr::ZarrV2Writer::flush_()
{
    if (bytes_to_flush_ == 0) {
        return;
    }

    // create chunk files if necessary
    if (files_.empty() &&
        !file_creator_.create_files(data_root_ / std::to_string(current_chunk_),
                                    array_spec_.dimensions,
                                    false,
                                    files_)) {
        return;
    }

    // compress buffers and write out
    compress_buffers_();
    std::latch latch(chunk_buffers_.size());
    {
        std::scoped_lock lock(buffers_mutex_);
        for (auto i = 0; i < files_.size(); ++i) {
            auto& chunk = chunk_buffers_.at(i);
            thread_pool_->push_to_job_queue(
              std::move([fh = &files_.at(i),
                         data = chunk.data(),
                         size = chunk.size(),
                         &latch](std::string& err) -> bool {
                  bool success = false;
                  try {
                      CHECK(file_write(fh, 0, data, data + size));
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

    // reset buffers
    const auto bytes_per_chunk = common::bytes_per_chunk(
      array_spec_.dimensions, array_spec_.image_shape.type);

    const auto bytes_to_reserve =
      bytes_per_chunk +
      (array_spec_.compression_params.has_value() ? BLOSC_MAX_OVERHEAD : 0);

    for (auto& buf : chunk_buffers_) {
        buf.clear();
        buf.reserve(bytes_to_reserve);
    }
    bytes_to_flush_ = 0;
}
