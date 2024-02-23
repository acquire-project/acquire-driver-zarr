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

bool
zarr::ZarrV3Writer::should_flush_() const noexcept
{
    return true;
}

bool
zarr::ZarrV3Writer::flush_impl_()
{
    // create shard files
    CHECK(files_.empty());
    if (files_.empty() && !file_creator_.create_shard_files(
                            data_root_ / ("c" + std::to_string(current_chunk_)),
                            array_spec_.dimensions,
                            files_)) {
        return false;
    }

    return true;

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
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        struct VideoFrame* frame = nullptr;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { throw std::runtime_error(err); });

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
                              2); // 2 / 2 = 1 shard
            dims.emplace_back("t",
                              DimensionType_Time,
                              0,
                              5,  // 5 timepoints / chunk
                              5); // 5 / 5 = 1 shard

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
                frame->frame_id = i;
                CHECK(writer.write(frame));
            }

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
