#ifndef ACQUIRE_DRIVER_ZARR_COMMON_H
#define ACQUIRE_DRIVER_ZARR_COMMON_H

#include "prelude.h"

#include "device/props/components.h"

namespace acquire::sink::zarr::common {
size_t
bytes_of_type(const SampleType& type);
} // namespace acquire::sink::zarr::common

#endif // ACQUIRE_DRIVER_ZARR_COMMON_H
