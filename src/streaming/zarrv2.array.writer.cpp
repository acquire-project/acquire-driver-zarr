#include "macros.hh"
#include "zarrv2.array.writer.hh"
#include "sink.creator.hh"
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
}

zarr::ZarrV2ArrayWriter::~ZarrV2ArrayWriter()
{
    is_finalizing_ = true;
    try {
        flush_();
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to finalize array writer: ", exc.what());
    } catch (...) {
        LOG_ERROR("Failed to finalize array writer: (unknown)");
    }
}

ZarrVersion
zarr::ZarrV2ArrayWriter::version_() const
{
    return ZarrVersion_2;
}

bool
zarr::ZarrV2ArrayWriter::flush_impl_()
{
    // create chunk files
    CHECK(data_sinks_.empty());
    if (!make_data_sinks_()) {
        return false;
    }

    CHECK(data_sinks_.size() == chunk_buffers_.size());

    std::latch latch(chunk_buffers_.size());
    {
        std::scoped_lock lock(buffers_mutex_);
        for (auto i = 0; i < data_sinks_.size(); ++i) {
            auto& chunk = chunk_buffers_.at(i);
            EXPECT(thread_pool_->push_job(
                     std::move([&sink = data_sinks_.at(i),
                                data_ = chunk.data(),
                                size = chunk.size(),
                                &latch](std::string& err) -> bool {
                         bool success = false;
                         try {
                             std::span data{
                                 reinterpret_cast<std::byte*>(data_), size
                             };
                             CHECK(sink->write(0, data));
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
    if (!make_metadata_sink_()) {
        return false;
    }

    using json = nlohmann::json;

    std::vector<size_t> array_shape, chunk_shape;

    size_t append_size = frames_written_;
    for (auto i = config_.dimensions->ndims() - 3; i > 0; --i) {
        const auto& dim = config_.dimensions->at(i);
        const auto& array_size_px = dim.array_size_px;
        CHECK(array_size_px);
        append_size = (append_size + array_size_px - 1) / array_size_px;
    }
    array_shape.push_back(append_size);

    chunk_shape.push_back(config_.dimensions->final_dim().chunk_size_px);
    for (auto i = 1; i < config_.dimensions->ndims(); ++i) {
        const auto& dim = config_.dimensions->at(i);
        array_shape.push_back(dim.array_size_px);
        chunk_shape.push_back(dim.chunk_size_px);
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

    std::string metadata_str = metadata.dump(4);
    std::span data{ reinterpret_cast<std::byte*>(metadata_str.data()),
                    metadata_str.size() };
    return metadata_sink_->write(0, data);
}

bool
zarr::ZarrV2ArrayWriter::should_rollover_() const
{
    return true;
}
