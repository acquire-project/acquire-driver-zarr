#ifndef ACQUIRE_STORAGE_ZARR_RAW_HH
#define ACQUIRE_STORAGE_ZARR_RAW_HH

#ifdef __cplusplus

#include "zarr.encoder.hh"

namespace acquire::sink::zarr {

struct RawEncoder final : public BaseEncoder
{
  public:
    RawEncoder();

    void set_file(struct file* file_handle) override;

  private:
    size_t file_offset_;

    size_t flush_impl() override;
};
} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // ACQUIRE_STORAGE_ZARR_RAW_HH
