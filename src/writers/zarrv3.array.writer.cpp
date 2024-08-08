#include "zarrv3.array.writer.hh"
#include "sink.creator.hh"
#include "zarr.hh"

#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

namespace {
std::string
sample_type_to_dtype(SampleType t)

{
    switch (t) {
        case SampleType_u8:
            return "uint8";
        case SampleType_u10:
        case SampleType_u12:
        case SampleType_u14:
        case SampleType_u16:
            return "uint16";
        case SampleType_i8:
            return "int8";
        case SampleType_i16:
            return "int16";
        case SampleType_f32:
            return "float32";
        default:
            throw std::runtime_error("Invalid SampleType: " +
                                     std::to_string(static_cast<int>(t)));
    }
}
} // end ::{anonymous} namespace

zarr::ZarrV3ArrayWriter::ZarrV3ArrayWriter(
  const ArrayWriterConfig& array_spec,
  std::shared_ptr<common::ThreadPool> thread_pool,
  std::shared_ptr<common::S3ConnectionPool> connection_pool)
  : ArrayWriter(array_spec, thread_pool, connection_pool)
  , shard_file_offsets_(common::number_of_shards(array_spec.dimensions), 0)
  , shard_tables_{ common::number_of_shards(array_spec.dimensions) }
{
    const auto chunks_per_shard =
      common::chunks_per_shard(array_spec.dimensions);

    for (auto& table : shard_tables_) {
        table.resize(2 * chunks_per_shard);
        std::fill_n(
          table.begin(), table.size(), std::numeric_limits<uint64_t>::max());
    }

    data_root_ = config_.dataset_root + "/data/root/" +
                 std::to_string(config_.level_of_detail);
    meta_root_ = config_.dataset_root + "/meta/root";
}

bool
zarr::ZarrV3ArrayWriter::flush_impl_()
{
    // create shard files if they don't exist
    const std::string data_root =
      data_root_ + "/c" + std::to_string(append_chunk_index_);

    {
        SinkCreator creator(thread_pool_, connection_pool_);
        if (data_sinks_.empty() &&
            !creator.make_data_sinks(data_root,
                                     config_.dimensions,
                                     common::shards_along_dimension,
                                     data_sinks_)) {
            return false;
        }
    }

    const auto n_shards = common::number_of_shards(config_.dimensions);
    CHECK(data_sinks_.size() == n_shards);

    // get shard indices for each chunk
    std::vector<std::vector<size_t>> chunk_in_shards(n_shards);
    for (auto i = 0; i < chunk_buffers_.size(); ++i) {
        const auto index = common::shard_index_for_chunk(i, config_.dimensions);
        chunk_in_shards.at(index).push_back(i);
    }

    // write out chunks to shards
    bool write_table = is_finalizing_ || should_rollover_();
    std::latch latch(n_shards);
    for (auto i = 0; i < n_shards; ++i) {
        const auto& chunks = chunk_in_shards.at(i);
        auto& chunk_table = shard_tables_.at(i);
        size_t* file_offset = &shard_file_offsets_.at(i);

        thread_pool_->push_to_job_queue([&sink = data_sinks_.at(i),
                                         &chunks,
                                         &chunk_table,
                                         file_offset,
                                         write_table,
                                         &latch,
                                         this](std::string& err) mutable {
            bool success = false;

            try {
                for (const auto& chunk_idx : chunks) {
                    auto& chunk = chunk_buffers_.at(chunk_idx);

                    success =
                      sink->write(*file_offset, chunk.data(), chunk.size());
                    if (!success) {
                        break;
                    }

                    const auto internal_idx = common::shard_internal_index(
                      chunk_idx, config_.dimensions);
                    chunk_table.at(2 * internal_idx) = *file_offset;
                    chunk_table.at(2 * internal_idx + 1) = chunk.size();

                    *file_offset += chunk.size();
                }

                if (success && write_table) {
                    const auto* table =
                      reinterpret_cast<const uint8_t*>(chunk_table.data());
                    success =
                      sink->write(*file_offset,
                                  table,
                                  chunk_table.size() * sizeof(uint64_t));
                }
            } catch (const std::exception& exc) {
                err = "Failed to write chunk: " + std::string(exc.what());
            } catch (...) {
                err = "Failed to write chunk: (unknown)";
            }

            latch.count_down();
            return success;
        });
    }

    // wait for all threads to finish
    latch.wait();

    // reset shard tables and file offsets
    if (write_table) {
        for (auto& table : shard_tables_) {
            std::fill_n(table.begin(),
                        table.size(),
                        std::numeric_limits<uint64_t>::max());
        }

        std::fill_n(shard_file_offsets_.begin(), shard_file_offsets_.size(), 0);
    }

    return true;
}

bool
zarr::ZarrV3ArrayWriter::write_array_metadata_()
{
    if (!metadata_sink_) {
        const std::string metadata_path =
          std::to_string(config_.level_of_detail) + ".array.json";
        SinkCreator creator(thread_pool_, connection_pool_);
        if (!(metadata_sink_ = creator.make_sink(meta_root_, metadata_path))) {
            LOGE("Failed to create metadata sink: %s/%s",
                 meta_root_.c_str(),
                 metadata_path.c_str());
            return false;
        }
    }

    namespace fs = std::filesystem;
    using json = nlohmann::json;

    const auto& image_shape = config_.image_shape;

    std::vector<size_t> array_shape, chunk_shape, shard_shape;

    size_t append_size = frames_written_;
    for (auto i = 2; i < config_.dimensions.size() - 1; ++i) {
        const auto& dim = config_.dimensions[i];
        CHECK(dim.array_size_px);
        append_size = (append_size + dim.array_size_px - 1) / dim.array_size_px;
    }
    array_shape.push_back(append_size);

    chunk_shape.push_back(config_.dimensions.back().chunk_size_px);
    shard_shape.push_back(config_.dimensions.back().shard_size_chunks);
    for (auto dim = config_.dimensions.rbegin() + 1;
         dim != config_.dimensions.rend();
         ++dim) {
        array_shape.push_back(dim->array_size_px);
        chunk_shape.push_back(dim->chunk_size_px);
        shard_shape.push_back(dim->shard_size_chunks);
    }

    json metadata;
    metadata["attributes"] = json::object();
    metadata["chunk_grid"] = json::object({
      { "chunk_shape", chunk_shape },
      { "separator", "/" },
      { "type", "regular" },
    });

    metadata["chunk_memory_layout"] = "C";
    metadata["data_type"] = sample_type_to_dtype(image_shape.type);
    metadata["extensions"] = json::array();
    metadata["fill_value"] = 0;
    metadata["shape"] = array_shape;

    if (config_.compression_params) {
        const auto params = *config_.compression_params;
        metadata["compressor"] = json::object({
          { "codec", "https://purl.org/zarr/spec/codec/blosc/1.0" },
          { "configuration",
            json::object({
              { "blocksize", 0 },
              { "clevel", params.clevel },
              { "cname", params.codec_id },
              { "shuffle", params.shuffle },
            }) },
        });
    }

    // sharding storage transformer
    // TODO (aliddell):
    // https://github.com/zarr-developers/zarr-python/issues/877
    metadata["storage_transformers"] = json::array();
    metadata["storage_transformers"][0] = json::object({
      { "type", "indexed" },
      { "extension",
        "https://purl.org/zarr/spec/storage_transformers/sharding/1.0" },
      { "configuration",
        json::object({
          { "chunks_per_shard", shard_shape },
        }) },
    });

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();

    return metadata_sink_->write(0, metadata_bytes, metadata_str.size());
}

bool
zarr::ZarrV3ArrayWriter::should_rollover_() const
{
    const auto& dims = config_.dimensions;
    size_t frames_before_flush =
      dims.back().chunk_size_px * dims.back().shard_size_chunks;
    for (auto i = 2; i < dims.size() - 1; ++i) {
        frames_before_flush *= dims[i].array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_ % frames_before_flush == 0;
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

        const unsigned int array_width = 64, array_height = 48,
                           array_planes = 6, array_channels = 8,
                           array_timepoints = 10;
        const unsigned int n_frames =
          array_planes * array_channels * array_timepoints;

        const unsigned int chunk_width = 16, chunk_height = 16,
                           chunk_planes = 2, chunk_channels = 4,
                           chunk_timepoints = 5;

        const unsigned int shard_width = 2, shard_height = 1, shard_planes = 1,
                           shard_channels = 2, shard_timepoints = 2;
        const unsigned int chunks_per_shard = shard_width * shard_height *
                                              shard_planes * shard_channels *
                                              shard_timepoints;

        const unsigned int chunks_in_x =
          (array_width + chunk_width - 1) / chunk_width; // 4 chunks
        const unsigned int chunks_in_y =
          (array_height + chunk_height - 1) / chunk_height; // 3 chunks
        const unsigned int chunks_in_z =
          (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks
        const unsigned int chunks_in_c =
          (array_channels + chunk_channels - 1) / chunk_channels; // 2 chunks
        const unsigned int chunks_in_t =
          (array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

        const unsigned int shards_in_x =
          (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
        const unsigned int shards_in_y =
          (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
        const unsigned int shards_in_z =
          (chunks_in_z + shard_planes - 1) / shard_planes; // 3 shards
        const unsigned int shards_in_c =
          (chunks_in_c + shard_channels - 1) / shard_channels; // 1 shard
        const unsigned int shards_in_t =
          (chunks_in_t + shard_timepoints - 1) / shard_timepoints; // 1 shard

        const ImageShape shape
          {
              .dims = {
                .width = array_width,
                .height = array_height,
              },
              .strides = {
                .width = 1,
                .height = array_width,
                .planes = array_width * array_height,
              },
              .type = SampleType_u16,
          };
        const unsigned int nbytes_px = bytes_of_type(shape.type);

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, array_width, chunk_width, shard_width);
            dims.emplace_back("y",
                              DimensionType_Space,
                              array_height,
                              chunk_height,
                              shard_height);
            dims.emplace_back("z",
                              DimensionType_Space,
                              array_planes,
                              chunk_planes,
                              shard_planes);
            dims.emplace_back("c",
                              DimensionType_Channel,
                              array_channels,
                              chunk_channels,
                              shard_channels);
            dims.emplace_back("t",
                              DimensionType_Time,
                              array_timepoints,
                              chunk_timepoints,
                              shard_timepoints);

            zarr::ArrayWriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .level_of_detail = 3,
                .dataset_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3ArrayWriter writer(
              config, thread_pool, std::shared_ptr<common::S3ConnectionPool>());

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector<uint8_t> data(frame_size, 0);

            for (auto i = 0; i < n_frames; ++i) {
                CHECK(writer.write(data.data(), frame_size));
            }
            writer.finalize();

            const auto chunk_size = chunk_width * chunk_height * chunk_planes *
                                    chunk_channels * chunk_timepoints *
                                    nbytes_px;
            const auto index_size = chunks_per_shard *
                                    sizeof(uint64_t) * // indices are 64 bits
                                    2;                 // 2 indices per chunk
            const auto expected_file_size = shard_width * shard_height *
                                              shard_planes * shard_channels *
                                              shard_timepoints * chunk_size +
                                            index_size;

            const fs::path data_root =
              base_dir / "data/root" / std::to_string(config.level_of_detail);
            CHECK(fs::is_directory(data_root));
            for (auto t = 0; t < shards_in_t; ++t) {
                const auto t_dir = data_root / ("c" + std::to_string(t));
                CHECK(fs::is_directory(t_dir));

                for (auto c = 0; c < shards_in_c; ++c) {
                    const auto c_dir = t_dir / std::to_string(c);
                    CHECK(fs::is_directory(c_dir));

                    for (auto z = 0; z < shards_in_z; ++z) {
                        const auto z_dir = c_dir / std::to_string(z);
                        CHECK(fs::is_directory(z_dir));

                        for (auto y = 0; y < shards_in_y; ++y) {
                            const auto y_dir = z_dir / std::to_string(y);
                            CHECK(fs::is_directory(y_dir));

                            for (auto x = 0; x < shards_in_x; ++x) {
                                const auto x_file = y_dir / std::to_string(x);
                                CHECK(fs::is_regular_file(x_file));
                                const auto file_size = fs::file_size(x_file);
                                CHECK(file_size == expected_file_size);
                            }

                            CHECK(!fs::is_regular_file(
                              y_dir / std::to_string(shards_in_x)));
                        }

                        CHECK(!fs::is_directory(z_dir /
                                                std::to_string(shards_in_y)));
                    }

                    CHECK(
                      !fs::is_directory(c_dir / std::to_string(shards_in_z)));
                }

                CHECK(!fs::is_directory(t_dir / std::to_string(shards_in_c)));
            }

            CHECK(!fs::is_directory(base_dir /
                                    ("c" + std::to_string(shards_in_t))));

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
        return retval;
    }

    acquire_export int unit_test__zarrv3_writer__write_ragged_append_dim()
    {
        int retval = 0;
        const fs::path base_dir = fs::temp_directory_path() / "acquire";

        const unsigned int array_width = 64, array_height = 48,
                           array_planes = 5;
        const unsigned int n_frames = array_planes;

        const unsigned int chunk_width = 16, chunk_height = 16,
                           chunk_planes = 2;

        const unsigned int shard_width = 2, shard_height = 1, shard_planes = 1;
        const unsigned int chunks_per_shard =
          shard_width * shard_height * shard_planes;

        const unsigned int chunks_in_x =
          (array_width + chunk_width - 1) / chunk_width; // 4 chunks
        const unsigned int chunks_in_y =
          (array_height + chunk_height - 1) / chunk_height; // 3 chunks
        const unsigned int chunks_in_z =
          (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks

        const unsigned int shards_in_x =
          (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
        const unsigned int shards_in_y =
          (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
        const unsigned int shards_in_z =
          (chunks_in_z + shard_planes - 1) / shard_planes; // 3 shards

        const ImageShape shape
          {
              .dims = {
                .width = array_width,
                .height = array_height,
              },
              .strides = {
                .width = 1,
                .height = array_width,
                .planes = array_width * array_height,
              },
              .type = SampleType_i8,
          };
        const unsigned int nbytes_px = bytes_of_type(shape.type);

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, array_width, chunk_width, shard_width);
            dims.emplace_back("y",
                              DimensionType_Space,
                              array_height,
                              chunk_height,
                              shard_height);
            dims.emplace_back("z",
                              DimensionType_Space,
                              array_planes,
                              chunk_planes,
                              shard_planes);

            zarr::ArrayWriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .level_of_detail = 4,
                .dataset_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3ArrayWriter writer(
              config, thread_pool, std::shared_ptr<common::S3ConnectionPool>());

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector<uint8_t> data(frame_size, 0);

            for (auto i = 0; i < n_frames; ++i) {
                CHECK(writer.write(data.data(), frame_size) == frame_size);
            }
            writer.finalize();

            const auto chunk_size =
              chunk_width * chunk_height * chunk_planes * nbytes_px;
            const auto index_size = chunks_per_shard *
                                    sizeof(uint64_t) * // indices are 64 bits
                                    2;                 // 2 indices per chunk
            const auto expected_file_size =
              shard_width * shard_height * shard_planes * chunk_size +
              index_size;

            const fs::path data_root =
              base_dir / "data/root" / std::to_string(config.level_of_detail);
            CHECK(fs::is_directory(data_root));
            for (auto z = 0; z < shards_in_z; ++z) {
                const auto z_dir = data_root / ("c" + std::to_string(z));
                CHECK(fs::is_directory(z_dir));

                for (auto y = 0; y < shards_in_y; ++y) {
                    const auto y_dir = z_dir / std::to_string(y);
                    CHECK(fs::is_directory(y_dir));

                    for (auto x = 0; x < shards_in_x; ++x) {
                        const auto x_file = y_dir / std::to_string(x);
                        CHECK(fs::is_regular_file(x_file));
                        const auto file_size = fs::file_size(x_file);
                        CHECK(file_size == expected_file_size);
                    }

                    CHECK(!fs::is_regular_file(y_dir /
                                               std::to_string(shards_in_x)));
                }

                CHECK(!fs::is_directory(z_dir / std::to_string(shards_in_y)));
            }

            CHECK(!fs::is_directory(base_dir /
                                    ("c" + std::to_string(shards_in_z))));

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
        return retval;
    }

    acquire_export int unit_test__zarrv3_writer__write_ragged_internal_dim()
    {
        int retval = 0;
        const fs::path base_dir = fs::temp_directory_path() / "acquire";

        const unsigned int array_width = 64, array_height = 48,
                           array_planes = 5, array_timepoints = 10;
        const unsigned int n_frames = array_planes * array_timepoints;

        const unsigned int chunk_width = 16, chunk_height = 16,
                           chunk_planes = 2, chunk_timepoints = 5;

        const unsigned int shard_width = 2, shard_height = 1, shard_planes = 1,
                           shard_timepoints = 2;
        const unsigned int chunks_per_shard =
          shard_width * shard_height * shard_planes * shard_timepoints;

        const unsigned int chunks_in_x =
          (array_width + chunk_width - 1) / chunk_width; // 4 chunks
        const unsigned int chunks_in_y =
          (array_height + chunk_height - 1) / chunk_height; // 3 chunks
        const unsigned int chunks_in_z =
          (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks, ragged
        const unsigned int chunks_in_t =
          (array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

        const unsigned int shards_in_x =
          (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
        const unsigned int shards_in_y =
          (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
        const unsigned int shards_in_z =
          (chunks_in_z + shard_planes - 1) / shard_planes; // 3 shards
        const unsigned int shards_in_t =
          (chunks_in_t + shard_timepoints - 1) / shard_timepoints; // 1 shard

        const ImageShape shape
          {
              .dims = {
                .width = array_width,
                .height = array_height,
              },
              .strides = {
                .width = 1,
                .height = array_width,
                .planes = array_width * array_height,
              },
              .type = SampleType_f32,
          };
        const unsigned int nbytes_px = bytes_of_type(shape.type);

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, array_width, chunk_width, shard_width);
            dims.emplace_back("y",
                              DimensionType_Space,
                              array_height,
                              chunk_height,
                              shard_height);
            dims.emplace_back("z",
                              DimensionType_Space,
                              array_planes,
                              chunk_planes,
                              shard_planes);
            dims.emplace_back("t",
                              DimensionType_Time,
                              array_timepoints,
                              chunk_timepoints,
                              shard_timepoints);

            zarr::ArrayWriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .level_of_detail = 5,
                .dataset_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV3ArrayWriter writer(
              config, thread_pool, std::shared_ptr<common::S3ConnectionPool>());

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector<uint8_t> data(frame_size, 0);

            for (auto i = 0; i < n_frames; ++i) {
                CHECK(writer.write(data.data(), frame_size) == frame_size);
            }
            writer.finalize();

            const auto chunk_size = chunk_width * chunk_height * chunk_planes *
                                    chunk_timepoints * nbytes_px;
            const auto index_size = chunks_per_shard *
                                    sizeof(uint64_t) * // indices are 64 bits
                                    2;                 // 2 indices per chunk
            const auto expected_file_size = shard_width * shard_height *
                                              shard_planes * shard_timepoints *
                                              chunk_size +
                                            index_size;

            const fs::path data_root =
              base_dir / "data/root" / std::to_string(config.level_of_detail);
            CHECK(fs::is_directory(data_root));
            for (auto t = 0; t < shards_in_t; ++t) {
                const auto t_dir = data_root / ("c" + std::to_string(t));
                CHECK(fs::is_directory(t_dir));

                for (auto z = 0; z < shards_in_z; ++z) {
                    const auto z_dir = t_dir / std::to_string(z);
                    CHECK(fs::is_directory(z_dir));

                    for (auto y = 0; y < shards_in_y; ++y) {
                        const auto y_dir = z_dir / std::to_string(y);
                        CHECK(fs::is_directory(y_dir));

                        for (auto x = 0; x < shards_in_x; ++x) {
                            const auto x_file = y_dir / std::to_string(x);
                            CHECK(fs::is_regular_file(x_file));
                            const auto file_size = fs::file_size(x_file);
                            CHECK(file_size == expected_file_size);
                        }

                        CHECK(!fs::is_regular_file(
                          y_dir / std::to_string(shards_in_x)));
                    }

                    CHECK(
                      !fs::is_directory(z_dir / std::to_string(shards_in_y)));
                }

                CHECK(!fs::is_directory(t_dir / std::to_string(shards_in_z)));
            }

            CHECK(!fs::is_directory(base_dir /
                                    ("c" + std::to_string(shards_in_t))));

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
        return retval;
    }
}
#endif
