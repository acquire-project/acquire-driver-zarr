#include "s3.sink.hh"

#include <regex>

#include <aws/s3/model/CompleteMultipartUploadRequest.h>

namespace zarr = acquire::sink::zarr;

template<>
void
zarr::sink_close<zarr::S3Sink>(acquire::sink::zarr::Sink* sink_)
{
    if (!sink_) {
        return;
    }

    if (auto* sink = dynamic_cast<zarr::S3Sink*>(sink_); !sink) {
        LOGE("Failed to cast Sink to S3Sink");
    } else {
        delete sink;
    }
}

zarr::S3Sink::S3Sink(const std::string& uri)
{
    Aws::Client::ClientConfiguration config;


}

zarr::S3Sink::~S3Sink()
{
    close_();
}

bool
zarr::S3Sink::write(size_t offset, const uint8_t* buf, size_t bytes_of_buf)
{
    return false;
}

void
zarr::S3Sink::close_()
{
    Aws::S3::Model::CompleteMultipartUploadRequest complete_mpu_request;
}