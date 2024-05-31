#ifndef H_ACQUIRE_ZARR_V2_WRITER_V0
#define H_ACQUIRE_ZARR_V2_WRITER_V0

#include "writer.hh"

namespace acquire::sink::zarr {
struct ZarrV2Writer : public Writer
{
  public:
    ZarrV2Writer();
    ~ZarrV2Writer() override = default;

  protected:
    std::string make_array_metadata_() const override;
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_ZARR_V2_WRITER_V0
