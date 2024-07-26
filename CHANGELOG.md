# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Support for writing to S3 buckets.

### Changed

- Calling `acquire_get_configuration` with a Zarr storage device now returns a URI of the storage device, with file://
  scheme indicator and absolute path, assuming localhost.

### Added

- Support for multiscale in Zarr V3 stores.

## [0.1.11](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.10..v0.1.11) - 2024-04-22

### Fixed

- Acquisition dimensions can be reconfigured without causing a crash.

## [0.1.10](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.9..v0.1.10) - 2024-03-29

### Added

- Support for nearly arbitrary dimension configurations.

### Changed

- Chunk size may now exceed the array size along any dimension.
- The number of chunks per shard along any dimension no longer needs to divide the total number of chunks along that
  dimension.

## [0.1.9](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.8..v0.1.9) - 2024-01-11

### Changed

- Switches to `acquire-common` from `acquire-core-libs`, `acquire-video-runtime`, and `acquire-driver-common`.

## [0.1.8](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.7..v0.1.8) - 2023-12-19

### Fixed

- Resets the device state when the device is stopped.
- Ensure that all properties are saved when `zarr_get` is called.

## [0.1.7](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.6..v0.1.7) - 2023-12-11

### Fixed

- Removes `noexcept` specifier from various methods that don't need it and whose implementations may throw exceptions.

### Changed

- The thread pool starts on `Zarr::start()` and shuts down on `Zarr::stop()`.

## [0.1.6](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.5...v0.1.6) - 2023-11-28

### Fixed

- A bug where trailing whitespace on otherwise valid JSON would fail to validate.

## [0.1.5](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.4...v0.1.5) - 2023-11-20

### Added

- Support for [Zarr v3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html).
- Support for
  the [sharding storage transformer](https://web.archive.org/web/20230213221154/https://zarr-specs.readthedocs.io/en/latest/extensions/storage-transformers/sharding/v1.0.html)
  in Zarr v3.
- Ship debug libs for C-Blosc on Linux and Mac.

### Changed

- Upgrades C-Blosc from v1.21.4 to v1.21.5.

### Fixed

- A bug where enabling multiscale without specifying the tile size would cause an error.
- Exceptions thrown off the main thread are now caught and logged, and Zarr throws an error in `append`.

## [0.1.4](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.3...v0.1.4) - 2023-08-11

### Fixed

- A bug where not specifying the chunk size causes tile dimensions to be set to zero.

## [0.1.3](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.2...v0.1.3) - 2023-07-28

### Added

- Support for writing multiscale OME-Zarr.

### Changed

- `ZarrV2Writer`s need to specify which multiscale layer they write to.
- The Zarr writer now validates that image and tile shapes are set and compatible with each other before the first
  append.

### Removed

- Noisy thread status messages.

### Fixed

- Check that the image shape has been set before complaining that the tile shape along any dimension is also zero.
- A bug where multibyte samples exhibited striping behavior due to being copied from the wrong offset in the source
  buffer.

## [0.1.2](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.1...v0.1.2) - 2023-06-23

### Added

- Nightly releases.
- Acquisitions using Zarr as a storage device can be chunked along the X, Y, and Z axes, and the number of bytes per
  chunk can also be specified.
    - Chunking can be configured via `storage_properties_set_chunking_props()`. See README for details.

## 0.1.1 - 2023-05-11
