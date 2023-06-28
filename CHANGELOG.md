# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Check that image and tile dimensions are set properly before the first append.

### Fixed

- Check that the image shape has been set before complaining that the tile shape along any dimension is also zero.

## [0.1.2](https://github.com/acquire-project/acquire-driver-zarr/compare/v0.1.1...v0.1.2) - 2023-06-23

### Added

- Nightly releases.
- Acquisitions using Zarr as a storage device can be chunked along the X, Y, and Z axes, and the number of bytes per
  chunk can also be specified.
    - Chunking can be configured via `storage_properties_set_chunking_props()`. See README for details.

## 0.1.1 - 2023-05-11
