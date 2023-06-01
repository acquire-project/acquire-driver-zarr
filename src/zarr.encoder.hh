#ifndef H_ACQUIRE_STORAGE_ZARR_ENCODER_V0
#define H_ACQUIRE_STORAGE_ZARR_ENCODER_V0

#ifdef __cplusplus

#include <string>
#include <vector>

#include "platform.h"

#include "prelude.h"
#include "json.hpp"

namespace acquire::sink::zarr {

struct BaseEncoder
{
  public:
    virtual ~BaseEncoder() noexcept;

    size_t write(const uint8_t* beg, const uint8_t* end);
    size_t flush();
    void set_bytes_per_pixel(size_t bpp);
    void allocate_buffer(size_t buf_size);

    void set_file_path(const std::string& file_path);
    void close_file();

  protected:
    std::vector<uint8_t> buf_;
    size_t cursor_;
    size_t bytes_per_pixel_;
    std::string path_;
    struct file* file_handle_;
    bool file_has_been_created_;

    BaseEncoder();
    void open_file();

    virtual size_t flush_impl() = 0;
    virtual void open_file_impl() = 0;
};
} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // H_ACQUIRE_STORAGE_ZARR_ENCODER_V0
