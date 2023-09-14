#include "raw.encoder.hh"

namespace zarr = acquire::sink::zarr;

size_t
zarr::RawEncoder::encode(uint8_t* bytes_out,
                         size_t nbytes_out,
                         uint8_t* bytes_in,
                         size_t nbytes_in) const
{
    CHECK(bytes_in);
    CHECK(bytes_out);

    CHECK(nbytes_in > 0);
    CHECK(nbytes_out >= nbytes_in);

    std::memcpy(bytes_out, bytes_in, nbytes_in);

    return nbytes_in;
}
