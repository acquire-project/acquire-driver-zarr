#include "zarrv3.writer.hh"
#include "../zarr.hh"

#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

zarr::ZarrV3Writer::ZarrV3Writer(
  const ArraySpec& array_spec,
  std::shared_ptr<common::ThreadPool> thread_pool)
  : Writer(array_spec, thread_pool)
{
}

void
zarr::ZarrV3Writer::flush_()
{
    if (bytes_to_flush_ == 0) {
        return;
    }
    //    if (bytes_to_flush_ == 0) {
    //        return;
    //    }
    //
    //    // create shard files if necessary
    //    if (files_.empty() && !file_creator_.create_files(
    //                            data_root_ / ("c" +
    //                            std::to_string(current_chunk_)),
    //                            array_spec_.dimensions,
    //                            true,
    //                            files_)) {
    //        return;
    //    }
    //
    //    const auto chunks_per_shard = chunks_per_shard_();
    //
    //    // compress buffers
    //    compress_buffers_();
    //    const size_t bytes_of_index = 2 * chunks_per_shard * sizeof(uint64_t);
    //
    //    const auto max_bytes_per_chunk =
    //      bytes_per_tile * frames_per_chunk_ +
    //      (blosc_compression_params_.has_value() ? BLOSC_MAX_OVERHEAD : 0);
    //
    //    // concatenate chunks into shards
    //    const auto n_shards = shards_per_frame_();
    //    std::latch latch(n_shards);
    //    for (auto i = 0; i < n_shards; ++i) {
    //        thread_pool_->push_to_job_queue(
    //          std::move([fh = &files_.at(i), chunks_per_shard, i, &latch,
    //          this](
    //                      std::string& err) {
    //              size_t chunk_index = 0;
    //              std::vector<uint64_t> chunk_indices;
    //              size_t offset = 0;
    //              bool success = false;
    //              try {
    //                  for (auto j = 0; j < chunks_per_shard; ++j) {
    //                      chunk_indices.push_back(chunk_index); // chunk
    //                      offset const auto k = i * chunks_per_shard + j;
    //
    //                      auto& chunk = chunk_buffers_.at(k);
    //                      chunk_index += chunk.size();
    //                      chunk_indices.push_back(chunk.size()); // chunk
    //                      extent
    //
    //                      file_write(
    //                        fh, offset, chunk.data(), chunk.data() +
    //                        chunk.size());
    //                      offset += chunk.size();
    //                  }
    //
    //                  // write the indices out at the end of the shard
    //                  const auto* indices =
    //                    reinterpret_cast<const
    //                    uint8_t*>(chunk_indices.data());
    //                  success = (bool)file_write(fh,
    //                                             offset,
    //                                             indices,
    //                                             indices +
    //                                             chunk_indices.size() *
    //                                                         sizeof(uint64_t));
    //              } catch (const std::exception& exc) {
    //                  char buf[128];
    //                  snprintf(
    //                    buf, sizeof(buf), "Failed to write chunk: %s",
    //                    exc.what());
    //                  err = buf;
    //              } catch (...) {
    //                  err = "Unknown error";
    //              }
    //
    //              latch.count_down();
    //              return success;
    //          }));
    //    }
    //
    //    // wait for all threads to finish
    //    latch.wait();
    //
    //    // reset buffers
    //    for (auto& buf : chunk_buffers_) {
    //        buf.clear();
    //        buf.reserve(max_bytes_per_chunk);
    //    }
    //    bytes_to_flush_ = 0;
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
    acquire_export int unit_test__zarrv3_writer__write()
    {
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        struct VideoFrame* frame = nullptr;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { throw std::runtime_error(err); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x",
                              DimensionType_Space,
                              20,
                              5,  // 20 / 5 = 4 chunks
                              2); // 4 / 2 = 2 shards
            dims.emplace_back("y",
                              DimensionType_Space,
                              6,
                              2,  // 6 / 2 = 3 chunks
                              1); // 3 / 1 = 3 shards
            dims.emplace_back("z",
                              DimensionType_Space,
                              15,
                              3,  // 15 / 3 = 5 chunks
                              1); // 5 / 1 = 5 shards
            dims.emplace_back("c",
                              DimensionType_Channel,
                              1,
                              1,  // 1 / 1 = 1 chunk
                              1); // 1 / 1 = 1 shard
            dims.emplace_back("t",
                              DimensionType_Time,
                              0,
                              2,
                              2); // 2 timepoints per chunk, 2 chunks per shard

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

            zarr::ZarrV3Writer writer(array_spec, thread_pool);

            frame = (VideoFrame*)malloc(sizeof(VideoFrame) + 64 * 48 * 2);
            frame->bytes_of_frame = sizeof(VideoFrame) + 64 * 48 * 2;
            frame->shape = shape;
            memset(frame->data, 0, 64 * 48 * 2);

            for (auto i = 0; i < 5 * 1 * 2; ++i) {
                CHECK(writer.write(frame));
            }

            CHECK(fs::is_directory(base_dir));
            for (auto c = 0; c < 1; ++c) {
                CHECK(fs::is_directory(base_dir / "0" / std::to_string(c)));
                for (auto z = 0; z < 5; ++z) {
                    CHECK(fs::is_directory(base_dir / "0" / std::to_string(c) /
                                           std::to_string(z)));
                    for (auto y = 0; y < 3; ++y) {
                        CHECK(fs::is_directory(
                          base_dir / "0" / std::to_string(c) /
                          std::to_string(z) / std::to_string(y)));
                        for (auto x = 0; x < 2; ++x) {
                            CHECK(fs::is_regular_file(
                              base_dir / "0" / std::to_string(c) /
                              std::to_string(z) / std::to_string(y) /
                              std::to_string(x)));
                        }
                    }
                }
            }

            // cleanup
            fs::remove_all(base_dir);
            free(frame);

            return 1;
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
        return 0;
    }
}
#endif
