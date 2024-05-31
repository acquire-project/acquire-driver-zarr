#ifndef H_ACQUIRE_ZARR_S3_WRITER_V0
#define H_ACQUIRE_ZARR_S3_WRITER_V0

#include "writer.hh"
#include "s3.sink.hh"

namespace acquire::sink::zarr {
struct S3Config
{
    std::string access_key_id;
    std::string secret_access_key;
};

struct S3Writer
{
    S3Writer(const std::string& data_root, const S3Config& s3_config);
    ~S3Writer() noexcept = default;

  protected:
    std::string endpoint_;
    std::string bucket_name_;
    std::string access_key_id_;
    std::string secret_access_key_;

    std::vector<std::unique_ptr<S3Sink>> sinks_;

    //    void close_() override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_S3_WRITER_V0
