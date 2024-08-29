#include "zarrv2.array.writer.hh"
#include "sink.creator.hh"
#include "logger.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>

#include <latch>
#include <stdexcept>

namespace {
std::string
sample_type_to_dtype(ZarrDataType t)

{
    const std::string dtype_prefix =
      std::endian::native == std::endian::big ? ">" : "<";

    switch (t) {
        case ZarrDataType_uint8:
            return dtype_prefix + "u1";
        case ZarrDataType_uint16:
            return dtype_prefix + "u2";
        case ZarrDataType_uint32:
            return dtype_prefix + "u4";
        case ZarrDataType_uint64:
            return dtype_prefix + "u8";
        case ZarrDataType_int8:
            return dtype_prefix + "i1";
        case ZarrDataType_int16:
            return dtype_prefix + "i2";
        case ZarrDataType_int32:
            return dtype_prefix + "i4";
        case ZarrDataType_int64:
            return dtype_prefix + "i8";
        case ZarrDataType_float16:
            return dtype_prefix + "f2";
        case ZarrDataType_float32:
            return dtype_prefix + "f4";
        case ZarrDataType_float64:
            return dtype_prefix + "f8";
        default:
            throw std::runtime_error("Invalid data type: " +
                                     std::to_string(static_cast<int>(t)));
    }
}
} // namespace

zarr::ZarrV2ArrayWriter::ZarrV2ArrayWriter(
  const ArrayWriterConfig& config,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : ArrayWriter(config, thread_pool, s3_connection_pool)
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
        SinkCreator creator(thread_pool_, s3_connection_pool_);
        if (!creator.make_data_sinks(data_root,
                                     config_.dimensions,
                                     chunks_along_dimension,
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
            EXPECT(thread_pool_->push_to_job_queue(
                     std::move([&sink = data_sinks_.at(i),
                                data = chunk.data(),
                                size = chunk.size(),
                                &latch](std::string& err) -> bool {
                         bool success = false;
                         try {
                             CHECK(sink->write(0, data, size));
                             success = true;
                         } catch (const std::exception& exc) {
                             err = "Failed to write chunk: " +
                                   std::string(exc.what());
                         } catch (...) {
                             err = "Failed to write chunk: (unknown)";
                         }

                         latch.count_down();
                         return success;
                     })),
                   "Failed to push job to thread pool");
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

        if (s3_connection_pool_) {
            SinkCreator creator(thread_pool_, s3_connection_pool_);
            metadata_sink_ = creator.make_sink(meta_root_, metadata_path);
        } else {
            metadata_sink_ =
              zarr::SinkCreator::make_sink(meta_root_ + "/" + metadata_path);
        }

        if (!metadata_sink_) {
            LOG_ERROR("Failed to create metadata sink: %s/%s",
                      meta_root_.c_str(),
                      metadata_path.c_str());
            return false;
        }
    }

    using json = nlohmann::json;

    std::vector<size_t> array_shape, chunk_shape;

    size_t append_size = frames_written_;
    for (auto dim = config_.dimensions.rbegin() + 2;
         dim < config_.dimensions.rend() - 1;
         ++dim) {
        CHECK(dim->array_size_px);
        append_size =
          (append_size + dim->array_size_px - 1) / dim->array_size_px;
    }
    array_shape.push_back(append_size);

    chunk_shape.push_back(config_.dimensions.front().chunk_size_px);
    for (auto dim = config_.dimensions.begin() + 1;
         dim != config_.dimensions.end();
         ++dim) {
        array_shape.push_back(dim->array_size_px);
        chunk_shape.push_back(dim->chunk_size_px);
    }

    json metadata;
    metadata["zarr_format"] = 2;
    metadata["shape"] = array_shape;
    metadata["chunks"] = chunk_shape;
    metadata["dtype"] = sample_type_to_dtype(config_.dtype);
    metadata["fill_value"] = 0;
    metadata["order"] = "C";
    metadata["filters"] = nullptr;
    metadata["dimension_separator"] = "/";

    if (config_.compression_params) {
        const BloscCompressionParams bcp = *config_.compression_params;
        metadata["compressor"] = json{ { "id", "blosc" },
                                       { "cname", bcp.codec_id },
                                       { "clevel", bcp.clevel },
                                       { "shuffle", bcp.shuffle } };
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
