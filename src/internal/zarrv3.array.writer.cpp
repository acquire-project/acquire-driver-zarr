#include "zarrv3.array.writer.hh"
#include "sink.creator.hh"
#include "logger.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>

#include <algorithm> // std::fill_n
#include <latch>
#include <stdexcept>

#ifdef max
#undef max
#endif

namespace {
std::string
sample_type_to_dtype(ZarrDataType t)

{
    switch (t) {
        case ZarrDataType_uint8:
            return "uint8";
        case ZarrDataType_uint16:
            return "uint16";
        case ZarrDataType_uint32:
            return "uint32";
        case ZarrDataType_uint64:
            return "uint64";
        case ZarrDataType_int8:
            return "int8";
        case ZarrDataType_int16:
            return "int16";
        case ZarrDataType_int32:
            return "int32";
        case ZarrDataType_int64:
            return "int64";
        case ZarrDataType_float16:
            return "float16";
        case ZarrDataType_float32:
            return "float32";
        case ZarrDataType_float64:
            return "float64";
        default:
            throw std::runtime_error("Invalid ZarrDataType: " +
                                     std::to_string(static_cast<int>(t)));
    }
}
} // end ::{anonymous} namespace

zarr::ZarrV3ArrayWriter::ZarrV3ArrayWriter(
  const ArrayWriterConfig& array_spec,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : ArrayWriter(array_spec, thread_pool, s3_connection_pool)
  , shard_file_offsets_(number_of_shards(array_spec.dimensions), 0)
  , shard_tables_{ number_of_shards(array_spec.dimensions) }
{
    const auto cps = chunks_per_shard(array_spec.dimensions);

    for (auto& table : shard_tables_) {
        table.resize(2 * cps);
        std::fill_n(
          table.begin(), table.size(), std::numeric_limits<uint64_t>::max());
    }
}

zarr::ZarrV3ArrayWriter::~ZarrV3ArrayWriter()
{
    is_finalizing_ = true;
    try {
        flush_();
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to finalize array writer: %s", exc.what());
    } catch (...) {
        LOG_ERROR("Failed to finalize array writer: (unknown)");
    }
}

ZarrVersion
zarr::ZarrV3ArrayWriter::version_() const
{
    return ZarrVersion_3;
}

bool
zarr::ZarrV3ArrayWriter::flush_impl_()
{
    // create shard files if they don't exist
    if (data_sinks_.empty() && !make_data_sinks_()) {
        return false;
    }

    const auto n_shards = number_of_shards(config_.dimensions);
    CHECK(data_sinks_.size() == n_shards);

    // get shard indices for each chunk
    std::vector<std::vector<size_t>> chunk_in_shards(n_shards);
    for (auto i = 0; i < chunk_buffers_.size(); ++i) {
        const auto index = shard_index_for_chunk(i, config_.dimensions);
        chunk_in_shards.at(index).push_back(i);
    }

    // write out chunks to shards
    bool write_table = is_finalizing_ || should_rollover_();
    std::latch latch(n_shards);
    for (auto i = 0; i < n_shards; ++i) {
        const auto& chunks = chunk_in_shards.at(i);
        auto& chunk_table = shard_tables_.at(i);
        size_t* file_offset = &shard_file_offsets_.at(i);

        EXPECT(thread_pool_->push_to_job_queue([&sink = data_sinks_.at(i),
                                                &chunks,
                                                &chunk_table,
                                                file_offset,
                                                write_table,
                                                &latch,
                                                this](
                                                 std::string& err) mutable {
            bool success = false;

            try {
                for (const auto& chunk_idx : chunks) {
                    auto& chunk = chunk_buffers_.at(chunk_idx);
                    success =
                      sink->write(*file_offset, chunk.data(), chunk.size());
                    if (!success) {
                        break;
                    }

                    const auto internal_idx =
                      shard_internal_index(chunk_idx, config_.dimensions);
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
        }),
               "Failed to push job to thread pool");
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
    if (!make_metadata_sink_()) {
        return false;
    }

    using json = nlohmann::json;

    std::vector<size_t> array_shape, chunk_shape, shard_shape;

    size_t append_size = frames_written_;
    for (auto dim = config_.dimensions.rbegin() + 2;
         dim < config_.dimensions.rend() - 1;
         ++dim) {
        CHECK(dim->array_size_px);
        append_size = (append_size + dim->array_size_px - 1) / dim->array_size_px;
    }
    array_shape.push_back(append_size);

    chunk_shape.push_back(config_.dimensions.front().chunk_size_px);
    shard_shape.push_back(config_.dimensions.front().shard_size_chunks);
    for (auto dim = config_.dimensions.begin() + 1;
         dim != config_.dimensions.end();
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
    metadata["data_type"] = sample_type_to_dtype(config_.dtype);
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
    } else {
        metadata["compressor"] = nullptr;
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
    const auto& append_dim = dims.front();
    size_t frames_before_flush =
      append_dim.chunk_size_px * append_dim.shard_size_chunks;
    for (auto i = 1; i < dims.size() - 2; ++i) {
        frames_before_flush *= dims[i].array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_ % frames_before_flush == 0;
}
