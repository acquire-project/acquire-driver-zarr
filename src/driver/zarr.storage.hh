#pragma once

#include "device/kit/storage.h"

#include "zarr.h"

#include <string>

namespace acquire::sink {
struct Zarr : public Storage
{
  public:
    Zarr(ZarrVersion version,
         ZarrCompressionCodec compression_codec,
         uint8_t compression_level,
         uint8_t shuffle);
    ~Zarr();

    /// Storage interface
    void set(const StorageProperties* props);
    void get(StorageProperties* props) const;
    void get_meta(StoragePropertyMetadata* meta) const;
    void start();
    void stop() noexcept;
    size_t append(const VideoFrame* frames, size_t nbytes);
    void reserve_image_shape(const ImageShape* shape);

  private:
    ZarrVersion version_;
    ZarrCompressionCodec compression_codec_;
    uint8_t compression_level_;
    uint8_t shuffle_;

    std::string external_metadata_json_;

    ZarrStreamSettings* stream_settings_;
    ZarrStream* stream_;
};
} // namespace acquire::sink
