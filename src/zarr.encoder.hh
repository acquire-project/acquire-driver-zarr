#ifndef H_ACQUIRE_STORAGE_ZARR_ENCODER_V0
#define H_ACQUIRE_STORAGE_ZARR_ENCODER_V0

#ifdef __cplusplus

#include <string>
#include <vector>

#include "platform.h"

#include "prelude.h"

namespace acquire::sink::zarr {

struct BaseEncoder
{
  public:
    BaseEncoder();
    virtual ~BaseEncoder() noexcept = default;

    size_t write(const uint8_t* beg, const uint8_t* end);
    size_t flush();
    void set_bytes_per_pixel(size_t bpp);
    void allocate_buffer(size_t buf_size);

    virtual void set_file(struct file* file_handle);

  protected:
    std::vector<uint8_t> buf_;
    size_t cursor_;
    size_t bytes_per_pixel_;
    std::string path_;
    struct file* file_; // non-owning

    virtual size_t flush_impl() = 0;
};
} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // H_ACQUIRE_STORAGE_ZARR_ENCODER_V0
