#ifndef H_ACQUIRE_STORAGE_ZARR_RAW_V0
#define H_ACQUIRE_STORAGE_ZARR_RAW_V0

#ifdef __cplusplus

#include "zarr.codec.hh"
#include <vector>
#include <string>
#include <platform.h>

namespace acquire::sink::zarr {

/// A wrapper around `struct file` that closes on destruct.
/// Implements Writer
struct RawFile final
{
    explicit RawFile(const std::string& filename);
    ~RawFile();

    size_t write(const uint8_t* beg, const uint8_t* end);
    size_t flush();
    std::string to_json() const;

    inline void set_bytes_per_pixel(size_t bpp) const {}

    inline const struct file& inner() const { return file_; }

  private:
    RawFile() = delete;
    size_t last_offset_;
    struct file file_;
};

} // end namespace acquire::storage:zarr

#endif //__cplusplus
#endif // H_ACQUIRE_STORAGE_ZARR_RAW_V0
