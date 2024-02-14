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

    // create chunk files
    CHECK(files_.empty());
    if (!file_creator_.create_files(data_root_ / std::to_string(current_chunk_),
                                    array_spec_.dimensions,
                                    false,
                                    files_)) {
        return;
    }
    CHECK(files_.size() == chunk_buffers_.size());

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

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

namespace common = zarr::common;

extern "C"
{
    acquire_export int unit_test__zarrv2_writer__write()
    {
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        struct VideoFrame* frame = nullptr;
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { throw std::runtime_error(err); });

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

            zarr::ArraySpec array_spec = {
                .image_shape = shape,
                .dimensions = dims,
                .data_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV2Writer writer(array_spec, thread_pool);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48 * 2);
            frame->bytes_of_frame = sizeof(VideoFrame) + 64 * 48 * 2;
            frame->shape = shape;
            memset(frame->data, 0, 64 * 48 * 2);

            for (auto i = 0; i < 6 * 8 * 5; ++i) {
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
            for (auto c = 0; c < 2; ++c) {
                CHECK(fs::is_directory(base_dir / "0" / std::to_string(c)));
                for (auto z = 0; z < 3; ++z) {
                    CHECK(fs::is_directory(base_dir / "0" / std::to_string(c) /
                                           std::to_string(z)));
                    for (auto y = 0; y < 3; ++y) {
                        CHECK(fs::is_directory(
                          base_dir / "0" / std::to_string(c) /
                          std::to_string(z) / std::to_string(y)));
                        for (auto x = 0; x < 4; ++x) {
                            const auto filename =
                              base_dir / "0" / std::to_string(c) /
                              std::to_string(z) / std::to_string(y) /
                              std::to_string(x);
                            CHECK(fs::is_regular_file(filename));
                            const auto file_size = fs::file_size(filename);
                            CHECK(file_size == expected_file_size);
                        }
                    }
                }
            }

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
