# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Support for writing multiscale OME-Zarr.

### Changed

- `ChunkWriter`s need to specify which multiscale layer they write to.
- The Zarr writer now validates that image and tile shapes are set and compatible with each other before the first
  append.

### Removed

- Noisy thread status messages.

### Fixed

- Check that the image shape has been set before complaining that the tile shape along any dimension is also zero.
- A bug where multibyte samples exhibited striping behavior due to being copied from the wrong offset in the source
  buffer.
- A bug where not specifying the chunk size causes tile dimensions to be set to zero.

## [0.1.2](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.1...v0.1.2) - 2023-06-23

### Added

- Nightly releases.
- Acquisitions using Zarr as a storage device can be chunked along the X, Y, and Z axes, and the number of bytes per
  chunk can also be specified.
    - Chunking can be configured via `storage_properties_set_chunking_props()`. See README for details.

## 0.1.1 - 2023-05-11
