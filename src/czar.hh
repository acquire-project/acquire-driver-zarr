#ifndef H_ACQUIRE_STORAGE_CZAR_V0
#define H_ACQUIRE_STORAGE_CZAR_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "device/kit/storage.h"

#include "prelude.h"
#include "zarr.hh"
#include "writers/writer.hh"

#include <filesystem>
#include <stdexcept>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {

template<int Version, Writer WriterT>
struct Czar final : StorageInterface
{
  public:
    Czar() = default;
    ~Czar() override = default;

    /// StorageInterface
    void set(const StorageProperties* props) override;
    void get(StorageProperties* props) const override;
    void get_meta(StoragePropertyMetadata* meta) const override;
    void start() override;
    int stop() noexcept override;
    size_t append(const VideoFrame* frames, size_t nbytes) override;
    void reserve_image_shape(const ImageShape* shape) override;

  private:
    using ChunkingProps = StorageProperties::storage_properties_chunking_s;
    using ChunkingMeta =
      StoragePropertyMetadata::storage_property_metadata_chunking_s;

    std::shared_ptr<WriterT> writer_;

    // changes on set
    fs::path dataset_root_;
    std::string external_metadata_json_;
    PixelScale pixel_scale_um_;
    uint64_t max_bytes_per_chunk_;
    bool enable_multiscale_;

    ImageShape image_shape_;

    void set_chunking(const ChunkingProps& props, const ChunkingMeta& meta);
};

/// Czar implementation

/// StorageInterface
template<int Version, Writer WriterT>
void
Czar<Version, WriterT>::set(const StorageProperties* props)
{
    CHECK(props);

    StoragePropertyMetadata meta{};
    get_meta(&meta);

    // checks the directory exists and is writable
    validate_props(props);
    dataset_root_ = as_path(*props);

    if (props->external_metadata_json.str)
        external_metadata_json_ = props->external_metadata_json.str;

    pixel_scale_um_ = props->pixel_scale_um;

    // chunking
    set_chunking(props->chunking, meta.chunking);

    // hang on to this until we have the image shape
    enable_multiscale_ = meta.chunking.supported && props->enable_multiscale;
}

template<int Version, Writer WriterT>
void
Czar<Version, WriterT>::get(StorageProperties* props) const
{
}

template<int Version, Writer WriterT>
void
Czar<Version, WriterT>::get_meta(StoragePropertyMetadata* meta) const
{
    CHECK(meta);
    *meta = {
        .chunking = {
          .supported = 1,
          .max_bytes_per_chunk = {
            .writable = 1,
            .low = (float)(16 << 20),
            .high = (float)(1 << 30),
            .type = PropertyType_FixedPrecision },
        },
        .multiscale = {
          .supported = (int)(Version == 2),
        }
    };
}

template<int Version, Writer WriterT>
void
Czar<Version, WriterT>::start()
{
}

template<int Version, Writer WriterT>
int
Czar<Version, WriterT>::stop() noexcept
{
    return 1;
}

template<int Version, Writer WriterT>
size_t
Czar<Version, WriterT>::append(const VideoFrame* frames, size_t nbytes)
{
    return 0;
}

template<int Version, Writer WriterT>
void
Czar<Version, WriterT>::reserve_image_shape(const ImageShape* shape)
{
    image_shape_ = *shape;
}

template<int Version, Writer WriterT>
void
Czar<Version, WriterT>::set_chunking(const ChunkingProps& props,
                                     const ChunkingMeta& meta)
{
    max_bytes_per_chunk_ = std::clamp(props.max_bytes_per_chunk,
                                      (uint64_t)meta.max_bytes_per_chunk.low,
                                      (uint64_t)meta.max_bytes_per_chunk.high);
}
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_CZAR_V0
