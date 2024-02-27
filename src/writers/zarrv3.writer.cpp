#include "zarrv3.writer.hh"
#include "../zarr.hh"

#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

namespace {
size_t
shard_index(size_t chunk_id,
            size_t dimension_index,
            const std::vector<zarr::Dimension>& dimensions)
{
    CHECK(dimension_index < dimensions.size());

    // make chunk strides
    std::vector<size_t> chunk_strides(1, 1);
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        chunk_strides.push_back(chunk_strides.back() *
                                zarr::common::chunks_along_dimension(dim));
    }

    // get chunk indices
    std::vector<size_t> chunk_lattice_indices;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        chunk_lattice_indices.push_back(chunk_id % chunk_strides.at(i + 1) /
                                        chunk_strides.at(i));
    }
    chunk_lattice_indices.push_back(chunk_id / chunk_strides.back());

    // make shard strides
    std::vector<size_t> shard_strides(1, 1);
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        shard_strides.push_back(shard_strides.back() *
                                zarr::common::shards_along_dimension(dim));
    }

    std::vector<size_t> shard_lattice_indices;
    for (auto i = 0; i < dimensions.size(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices.at(i) /
                                        dimensions.at(i).shard_size_chunks);
    }

    size_t index = 0;
    for (auto i = 0; i < dimensions.size(); ++i) {
        index += shard_lattice_indices.at(i) * shard_strides.at(i);
    }

    return index;
}
} // namespace

zarr::ZarrV3Writer::ZarrV3Writer(
  const ArraySpec& array_spec,
  std::shared_ptr<common::ThreadPool> thread_pool)
  : Writer(array_spec, thread_pool)
{
}

bool
zarr::ZarrV3Writer::should_flush_() const
{
    const auto& dims = array_spec_.dimensions;
    size_t frames_before_flush =
      dims.back().chunk_size_px * dims.back().shard_size_chunks;
    for (auto i = 2; i < dims.size() - 1; ++i) {
        frames_before_flush *= dims[i].array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_ % frames_before_flush == 0;
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
    const auto n_shards = common::number_of_shards(array_spec_.dimensions);
    CHECK(files_.size() == n_shards);

    const auto chunks_per_shard =
      common::chunks_per_shard(array_spec_.dimensions);

    // compress buffers
    compress_buffers_();
    const size_t bytes_of_index = 2 * chunks_per_shard * sizeof(uint64_t);

    // concatenate chunks into shards
    std::latch latch(n_shards);
    for (auto i = 0; i < n_shards; ++i) {
        thread_pool_->push_to_job_queue(
          [fh = &files_.at(i), chunks_per_shard, i, &latch, this](
            std::string& err) {
              size_t chunk_index = 0;
              std::vector<uint64_t> chunk_indices;
              size_t offset = 0;
              bool success = false;
              try {
                  for (auto j = 0; j < chunks_per_shard; ++j) {
                      chunk_indices.push_back(chunk_index); // chunk offset
                      const auto k = i * chunks_per_shard + j;

                      auto& chunk = chunk_buffers_.at(k);
                      chunk_index += chunk.size();
                      chunk_indices.push_back(chunk.size()); // chunk extent

                      file_write(
                        fh, offset, chunk.data(), chunk.data() + chunk.size());
                      offset += chunk.size();
                  }

                  // write the indices out at the end of the shard
                  const auto* indices =
                    reinterpret_cast<const uint8_t*>(chunk_indices.data());
                  success = (bool)file_write(fh,
                                             offset,
                                             indices,
                                             indices + chunk_indices.size() *
                                                         sizeof(uint64_t));
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

    return true;
}

std::vector<std::vector<size_t>>
zarr::ZarrV3Writer::chunks_by_shard_() const
{
    return {};
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
    acquire_export int unit_test__shard_index()
    {
        int retval = 0;
        try {
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

            CHECK(shard_index(2, 0, dims) == 1);
            CHECK(shard_index(2, 1, dims) == 1);
            CHECK(shard_index(2, 2, dims) == 1);
            CHECK(shard_index(2, 2, dims) == 1);
            CHECK(shard_index(2, 4, dims) == 1);
            CHECK(shard_index(3, 0, dims) == 1);
            CHECK(shard_index(3, 1, dims) == 1);
            CHECK(shard_index(3, 2, dims) == 1);
            CHECK(shard_index(3, 2, dims) == 1);
            CHECK(shard_index(3, 4, dims) == 1);
            CHECK(shard_index(38, 0, dims) == 1);
            CHECK(shard_index(38, 1, dims) == 1);
            CHECK(shard_index(38, 2, dims) == 1);
            CHECK(shard_index(38, 2, dims) == 1);
            CHECK(shard_index(38, 4, dims) == 1);
            CHECK(shard_index(39, 0, dims) == 1);
            CHECK(shard_index(39, 1, dims) == 1);
            CHECK(shard_index(39, 2, dims) == 1);
            CHECK(shard_index(39, 2, dims) == 1);
            CHECK(shard_index(39, 4, dims) == 1);
            CHECK(shard_index(74, 0, dims) == 1);
            CHECK(shard_index(74, 1, dims) == 1);
            CHECK(shard_index(74, 2, dims) == 1);
            CHECK(shard_index(74, 2, dims) == 1);
            CHECK(shard_index(74, 4, dims) == 1);
            CHECK(shard_index(75, 0, dims) == 1);
            CHECK(shard_index(75, 1, dims) == 1);
            CHECK(shard_index(75, 2, dims) == 1);
            CHECK(shard_index(75, 2, dims) == 1);
            CHECK(shard_index(75, 4, dims) == 1);
            CHECK(shard_index(110, 0, dims) == 1);
            CHECK(shard_index(110, 1, dims) == 1);
            CHECK(shard_index(110, 2, dims) == 1);
            CHECK(shard_index(110, 2, dims) == 1);
            CHECK(shard_index(110, 4, dims) == 1);
            CHECK(shard_index(111, 0, dims) == 1);
            CHECK(shard_index(111, 1, dims) == 1);
            CHECK(shard_index(111, 2, dims) == 1);
            CHECK(shard_index(111, 2, dims) == 1);
            CHECK(shard_index(111, 4, dims) == 1);

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        return retval;
    }
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

            for (auto i = 0; i < 6 * 8 * 5 * 2; ++i) {
                frame->frame_id = i;
                CHECK(writer.write(frame));
            }
            writer.finalize();

            const auto expected_file_size = 16 * 2 *   // x
                                              16 * 3 * // y
                                              2 * 3 *  // z
                                              4 * 2 *  // c
                                              5 * 2 +  // t
                                            8 * 8 +    // offsets of 8 chunks
                                            8 * 8;     // extents of 8 chunks

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
