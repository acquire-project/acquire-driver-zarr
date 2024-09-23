#pragma once

#include "device/kit/storage.h"

#include "acquire.zarr.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

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
    std::string store_path_;

    std::optional<std::string> s3_endpoint_;
    std::optional<std::string> s3_bucket_name_;
    std::optional<std::string> s3_access_key_id_;
    std::optional<std::string> s3_secret_access_key_;

    std::string custom_metadata_;

    ZarrDataType dtype_;

    ZarrCompressionCodec compression_codec_;
    uint8_t compression_level_;
    uint8_t compression_shuffle_;

    std::vector<std::string> dimension_names_;
    std::vector<ZarrDimensionProperties> dimensions_;

    bool multiscale_;

    ZarrStream* stream_;
};
} // namespace acquire::sink
