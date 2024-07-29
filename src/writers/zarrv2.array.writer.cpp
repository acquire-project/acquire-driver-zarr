#include "zarrv2.array.writer.hh"
#include "sink.creator.hh"
#include "zarr.hh"

#include <cmath>
#include <latch>
#include <stdexcept>

namespace zarr = acquire::sink::zarr;

namespace {

std::string
sample_type_to_dtype(SampleType t)

{
    const std::string dtype_prefix =
      std::endian::native == std::endian::big ? ">" : "<";

    switch (t) {
        case SampleType_u8:
            return dtype_prefix + "u1";
        case SampleType_u10:
        case SampleType_u12:
        case SampleType_u14:
        case SampleType_u16:
            return dtype_prefix + "u2";
        case SampleType_i8:
            return dtype_prefix + "i1";
        case SampleType_i16:
            return dtype_prefix + "i2";
        case SampleType_f32:
            return dtype_prefix + "f4";
        default:
            throw std::runtime_error("Invalid SampleType: " +
                                     std::to_string(static_cast<int>(t)));
    }
}
} // end ::{anonymous} namespace

zarr::ZarrV2ArrayWriter::ZarrV2ArrayWriter(
  const ArrayWriterConfig& config,
  std::shared_ptr<common::ThreadPool> thread_pool,
  std::shared_ptr<common::S3ConnectionPool> connection_pool)
  : ArrayWriter(config, thread_pool, connection_pool)
{
    data_root_ =
      config_.dataset_root + "/" + std::to_string(config_.level_of_detail);
    meta_root_ = data_root_;
}

bool
zarr::ZarrV2ArrayWriter::flush_impl_()
{
    // create chunk files
    CHECK(data_sinks_.empty());
    const std::string data_root =
      data_root_ + "/" + std::to_string(append_chunk_index_);

    {
        SinkCreator creator(thread_pool_, connection_pool_);
        if (!creator.make_data_sinks(data_root,
                                     config_.dimensions,
                                     common::chunks_along_dimension,
                                     data_sinks_)) {
            return false;
        }
    }

    CHECK(data_sinks_.size() == chunk_buffers_.size());

    std::latch latch(chunk_buffers_.size());
    {
        std::scoped_lock lock(buffers_mutex_);
        for (auto i = 0; i < data_sinks_.size(); ++i) {
            auto& chunk = chunk_buffers_.at(i);
            thread_pool_->push_to_job_queue(
              std::move([&sink = data_sinks_.at(i),
                         data = chunk.data(),
                         size = chunk.size(),
                         &latch](std::string& err) -> bool {
                  bool success = false;
                  try {
                      CHECK(sink->write(0, data, size));
                      success = true;
                  } catch (const std::exception& exc) {
                      err = "Failed to write chunk: " + std::string(exc.what());
                  } catch (...) {
                      err = "Failed to write chunk: (unknown)";
                  }

                  latch.count_down();
                  return success;
              }));
        }
    }

    // wait for all threads to finish
    latch.wait();

    return true;
}

bool
zarr::ZarrV2ArrayWriter::write_array_metadata_()
{
    if (!metadata_sink_) {
        const std::string metadata_path = ".zarray";
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

    std::vector<size_t> array_shape, chunk_shape;

    array_shape.push_back(frames_written_);
    chunk_shape.push_back(config_.dimensions.back().chunk_size_px);
    for (auto dim = config_.dimensions.rbegin() + 1;
         dim != config_.dimensions.rend();
         ++dim) {
        array_shape.push_back(dim->array_size_px);
        chunk_shape.push_back(dim->chunk_size_px);
    }

    json metadata;
    metadata["zarr_format"] = 2;
    metadata["shape"] = array_shape;
    metadata["chunks"] = chunk_shape;
    metadata["dtype"] = sample_type_to_dtype(image_shape.type);
    metadata["fill_value"] = 0;
    metadata["order"] = "C";
    metadata["filters"] = nullptr;
    metadata["dimension_separator"] = "/";

    if (config_.compression_params) {
        metadata["compressor"] = *config_.compression_params;
    } else {
        metadata["compressor"] = nullptr;
    }

    const std::string metadata_str = metadata.dump(4);
    const auto* metadata_bytes = (const uint8_t*)metadata_str.c_str();

    return metadata_sink_->write(0, metadata_bytes, metadata_str.size());
}

bool
zarr::ZarrV2ArrayWriter::should_rollover_() const
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

        const unsigned int chunks_in_x =
          (array_width + chunk_width - 1) / chunk_width; // 4 chunks
        const unsigned int chunks_in_y =
          (array_height + chunk_height - 1) / chunk_height; // 3 chunks

        const unsigned int chunks_in_z =
          (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks
        const unsigned int chunks_in_c =
          (array_channels + chunk_channels - 1) / chunk_channels; // 2 chunks
        const unsigned int chunks_in_t =
          (array_timepoints + chunk_timepoints - 1) /
          chunk_timepoints; // 2 chunks

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
              [](const std::string& err) { LOGE("Error: %s\n", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, array_width, chunk_width, 0);
            dims.emplace_back(
              "y", DimensionType_Space, array_height, chunk_height, 0);
            dims.emplace_back(
              "z", DimensionType_Space, array_planes, chunk_planes, 0);
            dims.emplace_back(
              "c", DimensionType_Channel, array_channels, chunk_channels, 0);
            dims.emplace_back(
              "t", DimensionType_Time, array_timepoints, chunk_timepoints, 0);

            zarr::ArrayWriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .level_of_detail = 0,
                .dataset_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV2ArrayWriter writer(config, thread_pool, nullptr);

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector<uint8_t> data(frame_size, 0);

            for (auto i = 0; i < n_frames; ++i) { // 2 time points
                CHECK(writer.write(data.data(), frame_size));
            }
            writer.finalize();

            const auto expected_file_size = chunk_width * chunk_height *
                                            chunk_planes * chunk_channels *
                                            chunk_timepoints * nbytes_px;

            const fs::path data_root =
              base_dir / std::to_string(config.level_of_detail);
            CHECK(fs::is_directory(data_root));
            for (auto t = 0; t < chunks_in_t; ++t) {
                const auto t_dir = data_root / std::to_string(t);
                CHECK(fs::is_directory(t_dir));

                for (auto c = 0; c < chunks_in_c; ++c) {
                    const auto c_dir = t_dir / std::to_string(c);
                    CHECK(fs::is_directory(c_dir));

                    for (auto z = 0; z < chunks_in_z; ++z) {
                        const auto z_dir = c_dir / std::to_string(z);
                        CHECK(fs::is_directory(z_dir));

                        for (auto y = 0; y < chunks_in_y; ++y) {
                            const auto y_dir = z_dir / std::to_string(y);
                            CHECK(fs::is_directory(y_dir));

                            for (auto x = 0; x < chunks_in_x; ++x) {
                                const auto x_file = y_dir / std::to_string(x);
                                CHECK(fs::is_regular_file(x_file));
                                const auto file_size = fs::file_size(x_file);
                                CHECK(file_size == expected_file_size);
                            }

                            CHECK(!fs::is_regular_file(
                              y_dir / std::to_string(chunks_in_x)));
                        }

                        CHECK(!fs::is_directory(z_dir /
                                                std::to_string(chunks_in_y)));
                    }

                    CHECK(
                      !fs::is_directory(c_dir / std::to_string(chunks_in_z)));
                }

                CHECK(!fs::is_directory(t_dir / std::to_string(chunks_in_c)));
            }

            CHECK(!fs::is_directory(base_dir / std::to_string(chunks_in_t)));

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

    acquire_export int unit_test__zarrv2_writer__write_ragged_append_dim()
    {
        int retval = 0;
        const fs::path base_dir = fs::temp_directory_path() / "acquire";

        const unsigned int array_width = 64, array_height = 48,
                           array_planes = 5;
        const unsigned int n_frames = array_planes;

        const unsigned int chunk_width = 16, chunk_height = 16,
                           chunk_planes = 2;

        const unsigned int chunks_in_x =
          (array_width + chunk_width - 1) / chunk_width; // 4 chunks
        const unsigned int chunks_in_y =
          (array_height + chunk_height - 1) / chunk_height; // 3 chunks

        const unsigned int chunks_in_z =
          (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks, ragged

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
              .type = SampleType_u8,
          };
        const unsigned int nbytes_px = bytes_of_type(shape.type);

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, array_width, chunk_width, 0);
            dims.emplace_back(
              "y", DimensionType_Space, array_height, chunk_height, 0);
            dims.emplace_back(
              "z", DimensionType_Space, array_planes, chunk_planes, 0);

            zarr::ArrayWriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .level_of_detail = 1,
                .dataset_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV2ArrayWriter writer(
              config, thread_pool, std::shared_ptr<common::S3ConnectionPool>());

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector<uint8_t> data(frame_size, 0);

            for (auto i = 0; i < n_frames; ++i) {
                CHECK(writer.write(data.data(), frame_size) == frame_size);
            }
            writer.finalize();

            const auto expected_file_size =
              chunk_width * chunk_height * chunk_planes;

            const fs::path data_root =
              base_dir / std::to_string(config.level_of_detail);
            CHECK(fs::is_directory(data_root));
            for (auto z = 0; z < chunks_in_z; ++z) {
                const auto z_dir = data_root / std::to_string(z);
                CHECK(fs::is_directory(z_dir));

                for (auto y = 0; y < chunks_in_y; ++y) {
                    const auto y_dir = z_dir / std::to_string(y);
                    CHECK(fs::is_directory(y_dir));

                    for (auto x = 0; x < chunks_in_x; ++x) {
                        const auto x_file = y_dir / std::to_string(x);
                        CHECK(fs::is_regular_file(x_file));
                        const auto file_size = fs::file_size(x_file);
                        CHECK(file_size == expected_file_size);
                    }

                    CHECK(!fs::is_regular_file(y_dir /
                                               std::to_string(chunks_in_x)));
                }

                CHECK(!fs::is_directory(z_dir / std::to_string(chunks_in_y)));
            }

            CHECK(!fs::is_directory(base_dir / std::to_string(chunks_in_z)));

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

    acquire_export int unit_test__zarrv2_writer__write_ragged_internal_dim()
    {
        int retval = 0;
        const fs::path base_dir = fs::temp_directory_path() / "acquire";

        const unsigned int array_width = 64, array_height = 48,
                           array_planes = 5, array_timepoints = 5;
        const unsigned int n_frames = array_planes * array_timepoints;

        const unsigned int chunk_width = 16, chunk_height = 16,
                           chunk_planes = 2, chunk_timepoints = 5;

        const unsigned int chunks_in_x =
          (array_width + chunk_width - 1) / chunk_width; // 4 chunks
        const unsigned int chunks_in_y =
          (array_height + chunk_height - 1) / chunk_height; // 3 chunks

        const unsigned int chunks_in_z =
          (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks, ragged
        const unsigned int chunks_in_t =
          (array_timepoints + chunk_timepoints - 1) /
          chunk_timepoints; // 1 chunk

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
              .type = SampleType_u8,
          };
        const unsigned int nbytes_px = bytes_of_type(shape.type);

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, array_width, chunk_width, 0);
            dims.emplace_back(
              "y", DimensionType_Space, array_height, chunk_height, 0);
            dims.emplace_back(
              "z", DimensionType_Space, array_planes, chunk_planes, 0);
            dims.emplace_back(
              "t", DimensionType_Time, array_timepoints, chunk_timepoints, 0);

            zarr::ArrayWriterConfig config = {
                .image_shape = shape,
                .dimensions = dims,
                .level_of_detail = 2,
                .dataset_root = base_dir.string(),
                .compression_params = std::nullopt,
            };

            zarr::ZarrV2ArrayWriter writer(
              config, thread_pool, std::shared_ptr<common::S3ConnectionPool>());

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector<uint8_t> data(frame_size, 0);

            for (auto i = 0; i < n_frames; ++i) {
                CHECK(writer.write(data.data(), frame_size) == frame_size);
            }
            writer.finalize();

            const auto expected_file_size =
              chunk_width * chunk_height * chunk_planes * chunk_timepoints;

            const fs::path data_root =
              base_dir / std::to_string(config.level_of_detail);
            CHECK(fs::is_directory(data_root));
            for (auto t = 0; t < chunks_in_t; ++t) {
                const auto t_dir = data_root / std::to_string(t);
                CHECK(fs::is_directory(t_dir));

                for (auto z = 0; z < chunks_in_z; ++z) {
                    const auto z_dir = t_dir / std::to_string(z);
                    CHECK(fs::is_directory(z_dir));

                    for (auto y = 0; y < chunks_in_y; ++y) {
                        const auto y_dir = z_dir / std::to_string(y);
                        CHECK(fs::is_directory(y_dir));

                        for (auto x = 0; x < chunks_in_x; ++x) {
                            const auto x_file = y_dir / std::to_string(x);
                            CHECK(fs::is_regular_file(x_file));
                            const auto file_size = fs::file_size(x_file);
                            CHECK(file_size == expected_file_size);
                        }

                        CHECK(!fs::is_regular_file(
                          y_dir / std::to_string(chunks_in_x)));
                    }

                    CHECK(
                      !fs::is_directory(z_dir / std::to_string(chunks_in_y)));
                }

                CHECK(!fs::is_directory(t_dir / std::to_string(chunks_in_z)));
            }
            CHECK(!fs::is_directory(base_dir / std::to_string(chunks_in_t)));

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
