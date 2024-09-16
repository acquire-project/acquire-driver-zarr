#pragma once

#include "stream.settings.hh"

#include <cstddef> // size_t
#include <memory>  // unique_ptr

struct ZarrStream_s
{
  public:
    ZarrStream_s(struct ZarrStreamSettings_s* settings, ZarrVersion version);
    ~ZarrStream_s() = default;

    /**
     * @brief Append data to the stream.
     * @param data The data to append.
     * @param nbytes The number of bytes to append.
     * @return The number of bytes appended.
     */
    size_t append(const void* data, size_t nbytes);

    ZarrVersion version() const { return version_; }
    const ZarrStreamSettings_s& settings() const { return settings_; }

  private:
    struct ZarrStreamSettings_s settings_;
    ZarrVersion version_;   // Zarr version. 2 or 3.
    std::string error_; // error message. If nonempty, an error occurred.
};
