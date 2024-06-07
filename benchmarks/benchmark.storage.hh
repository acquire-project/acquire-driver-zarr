#ifndef ACQUIRE_DRIVER_ZARR_BENCHMARK_STORAGE_HH
#define ACQUIRE_DRIVER_ZARR_BENCHMARK_STORAGE_HH
#include "device/hal/storage.h"

#include <string>
#include <vector>

void
benchmark_storage(const std::string& storage_name,
                  std::vector<struct StorageProperties>& props);

#endif // ACQUIRE_DRIVER_ZARR_BENCHMARK_STORAGE_HH
