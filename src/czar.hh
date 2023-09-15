#ifndef H_ACQUIRE_STORAGE_CZAR_V0
#define H_ACQUIRE_STORAGE_CZAR_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "device/kit/storage.h"

#include "prelude.h"
#include "json.hpp"
#include "writers/writer.hh"
#include "writers/chunk.writer.hh"

#include <filesystem>
#include <stdexcept>
#include <vector>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
enum class BloscCodecId : uint8_t
{
    Lz4 = 1,
    Zstd = 5
};

template<BloscCodecId CodecId>
constexpr const char*
compression_codec_as_string();

template<>
constexpr const char*
compression_codec_as_string<BloscCodecId::Zstd>()
{
    return "zstd";
}

template<>
constexpr const char*
compression_codec_as_string<BloscCodecId::Lz4>()
{
    return "lz4";
}

struct CompressionParams
{
    static constexpr char id_[] = "blosc";
    std::string codec_id_;
    int clevel_;
    int shuffle_;

    CompressionParams();
    CompressionParams(const std::string& codec_id, int clevel, int shuffle);
};

void
to_json(nlohmann::json&, const CompressionParams&);

void
from_json(const nlohmann::json&, CompressionParams&);

// StorageInterface

struct StorageInterface : public Storage
{
    StorageInterface();
    virtual ~StorageInterface() = default;
    virtual void set(const StorageProperties* props) = 0;
    virtual void get(StorageProperties* props) const = 0;
    virtual void get_meta(StoragePropertyMetadata* meta) const = 0;
    virtual void start() = 0;
    virtual int stop() noexcept = 0;

    /// @return number of consumed bytes
    virtual size_t append(const VideoFrame* frames, size_t nbytes) = 0;

    /// @brief Set the image shape for allocating chunk writers.
    virtual void reserve_image_shape(const ImageShape* shape) = 0;
};

struct Czar : StorageInterface
{
  public:
    Czar() = default;
    Czar(CompressionParams&& compression_params);
    ~Czar() override = default;

    /// StorageInterface
    void set(const StorageProperties* props) override;
    void get(StorageProperties* props) const override;
    void start() override;
    int stop() noexcept override;
    size_t append(const VideoFrame* frames, size_t nbytes) override;
    void reserve_image_shape(const ImageShape* shape) override;

  protected:
    using ChunkingProps = StorageProperties::storage_properties_chunking_s;
    using ChunkingMeta =
      StoragePropertyMetadata::storage_property_metadata_chunking_s;

    std::vector<std::shared_ptr<ChonkWriter>> writers_;

    // static - set on construction
    std::optional<CompressionParams> compression_params_;

    // changes on set
    fs::path dataset_root_;
    std::string external_metadata_json_;
    PixelScale pixel_scale_um_;
    uint64_t max_bytes_per_chunk_;
    bool enable_multiscale_;

    ImageDims image_shape_;
    ImageDims tile_shape_;
    SampleType pixel_type_;

    /// Setup
    void set_chunking(const ChunkingProps& props, const ChunkingMeta& meta);
    virtual void allocate_writers_() = 0;

    /// Metadata
    void write_all_array_metadata_() const;
    virtual void write_array_metadata_(size_t level,
                                       const ImageDims& image_shape,
                                       const ImageDims& tile_shape) const = 0;
    virtual void write_external_metadata_() const = 0;
    virtual void write_base_metadata_() const = 0;
    virtual void write_group_metadata_() const = 0;

    /// Filesystem
    virtual std::string get_data_directory_() const = 0;
    virtual std::string get_chunk_dir_prefix_() const = 0;
};

} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_CZAR_V0
