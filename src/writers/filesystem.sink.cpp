#include "filesystem.sink.hh"

#include <latch>

namespace zarr = acquire::sink::zarr;

template<>
zarr::FilesystemSink*
zarr::sink_open(const std::string& uri)
{
    return new FilesystemSink(uri);
}

template<>
void
zarr::sink_close(FilesystemSink* sink)
{
    file_close(&sink->file_);
    delete sink;
}

zarr::FilesystemSink::FilesystemSink(const std::string& uri)
  : file_{}
{
    CHECK(file_create(&file_, uri.c_str(), uri.size() + 1));
}

zarr::FilesystemSink::~FilesystemSink()
{
    file_close(&file_);
}

bool
zarr::FilesystemSink::write(size_t offset,
                            const uint8_t* buf,
                            size_t bytes_of_buf)
{
    return file_write(&file_, offset, buf, buf + bytes_of_buf);
}

zarr::FileCreator::FileCreator(std::shared_ptr<common::ThreadPool> thread_pool)
  : thread_pool_(thread_pool)
{
}

bool
zarr::FileCreator::create_chunk_sinks(const std::string& base_uri,
                                      const std::vector<Dimension>& dimensions,
                                      std::vector<FilesystemSink*>& chunk_sinks)
{
    const std::string base_dir =
      base_uri.starts_with("file://") ? base_uri.substr(7) : base_uri;

    std::queue<fs::path> paths;
    paths.push(base_dir);

    if (!make_dirs_(paths)) {
        return false;
    }

    // create directories
    for (auto i = dimensions.size() - 2; i >= 1; --i) {
        const auto& dim = dimensions.at(i);
        const auto n_chunks = common::chunks_along_dimension(dim);

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_chunks; ++k) {
                paths.push(path / std::to_string(k));
            }
        }

        if (!make_dirs_(paths)) {
            return false;
        }
    }

    // create files
    {
        const auto& dim = dimensions.front();
        const auto n_chunks = common::chunks_along_dimension(dim);

        auto n_paths = paths.size();
        for (auto i = 0; i < n_paths; ++i) {
            const auto path = paths.front();
            paths.pop();
            for (auto j = 0; j < n_chunks; ++j) {
                paths.push(path / std::to_string(j));
            }
        }
    }

    return make_files_(paths, chunk_sinks);
}

bool
zarr::FileCreator::create_shard_sinks(const std::string& base_uri,
                                      const std::vector<Dimension>& dimensions,
                                      std::vector<FilesystemSink*>& shard_sinks)
{
    const std::string base_dir =
      base_uri.starts_with("file://") ? base_uri.substr(7) : base_uri;

    std::queue<fs::path> paths;
    paths.push(base_dir);

    if (!make_dirs_(paths)) {
        return false;
    }

    // create directories
    for (auto i = dimensions.size() - 2; i >= 1; --i) {
        const auto& dim = dimensions.at(i);
        const auto n_shards = common::shards_along_dimension(dim);
        CHECK(n_shards);

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_shards; ++k) {
                paths.push(path / std::to_string(k));
            }
        }

        if (!make_dirs_(paths)) {
            return false;
        }
    }

    // create files
    {
        const auto& dim = dimensions.front();
        const auto n_shards = common::shards_along_dimension(dim);
        CHECK(n_shards);

        auto n_paths = paths.size();
        for (auto i = 0; i < n_paths; ++i) {
            const auto path = paths.front();
            paths.pop();
            for (auto j = 0; j < n_shards; ++j) {
                paths.push(path / std::to_string(j));
            }
        }
    }

    return make_files_(paths, shard_sinks);
}

bool
zarr::FileCreator::create_metadata_sinks(
  const std::vector<std::string>& paths,
  std::vector<FilesystemSink*>& metadata_sinks)
{
    if (paths.empty()) {
        return true;
    }

    std::queue<fs::path> file_paths;
    for (const auto& path : paths) {
        fs::path p = path;
        fs::create_directories(p.parent_path());
        file_paths.push(p);
    }

    return make_files_(file_paths, metadata_sinks);
}

bool
zarr::FileCreator::make_dirs_(std::queue<fs::path>& dir_paths)
{
    if (dir_paths.empty()) {
        return true;
    }

    std::atomic<bool> all_successful = true;

    const auto n_dirs = dir_paths.size();
    std::latch latch(n_dirs);

    for (auto i = 0; i < n_dirs; ++i) {
        const auto dirname = dir_paths.front();
        dir_paths.pop();

        thread_pool_->push_to_job_queue(
          [dirname, &latch, &all_successful](std::string& err) -> bool {
              bool success = false;

              try {
                  if (fs::exists(dirname)) {
                      EXPECT(fs::is_directory(dirname),
                             "'%s' exists but is not a directory",
                             dirname.c_str());
                  } else if (all_successful) {
                      std::error_code ec;
                      EXPECT(fs::create_directories(dirname, ec),
                             "%s",
                             ec.message().c_str());
                  }
                  success = true;
              } catch (const std::exception& exc) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create directory '%s': %s.",
                           dirname.string().c_str(),
                           exc.what());
                  err = buf;
              } catch (...) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create directory '%s': (unknown).",
                           dirname.string().c_str());
                  err = buf;
              }

              latch.count_down();
              all_successful = all_successful && success;

              return success;
          });

        dir_paths.push(dirname);
    }

    latch.wait();

    return all_successful;
}

bool
zarr::FileCreator::make_files_(std::queue<fs::path>& file_paths,
                               std::vector<FilesystemSink*>& files)
{
    if (file_paths.empty()) {
        return true;
    }

    std::atomic<bool> all_successful = true;

    const auto n_files = file_paths.size();
    files.resize(n_files);
    std::fill(files.begin(), files.end(), nullptr);
    std::latch latch(n_files);

    for (auto i = 0; i < n_files; ++i) {
        const auto filename = file_paths.front();
        file_paths.pop();

        FilesystemSink** psink = files.data() + i;

        thread_pool_->push_to_job_queue(
          [filename, psink, &latch, &all_successful](std::string& err) -> bool {
              bool success = false;

              try {
                  if (all_successful) {
                      *psink = sink_open<FilesystemSink>(filename.string());
                  }
                  success = true;
              } catch (const std::exception& exc) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create file '%s': %s.",
                           filename.string().c_str(),
                           exc.what());
                  err = buf;
              } catch (...) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create file '%s': (unknown).",
                           filename.string().c_str());
                  err = buf;
              }

              latch.count_down();
              all_successful = all_successful && success;

              return success;
          });
    }

    latch.wait();

    return all_successful;
}

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif

namespace common = zarr::common;

extern "C"
{
    acquire_export int unit_test__file_creator__create_chunk_sinks()
    {
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s\n", err.c_str()); });
            zarr::FileCreator file_creator{ thread_pool };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x", DimensionType_Space, 10, 2, 0); // 5 chunks
            dims.emplace_back("y", DimensionType_Space, 4, 2, 0);  // 2 chunks
            dims.emplace_back(
              "z", DimensionType_Space, 0, 3, 0); // 3 timepoints per chunk

            std::vector<zarr::FilesystemSink*> files;
            CHECK(
              file_creator.create_chunk_sinks(base_dir.string(), dims, files));

            CHECK(files.size() == 5 * 2);
            std::for_each(files.begin(),
                          files.end(),
                          [](zarr::FilesystemSink* f) { sink_close(f); });

            CHECK(fs::is_directory(base_dir));
            for (auto y = 0; y < 2; ++y) {
                CHECK(fs::is_directory(base_dir / std::to_string(y)));
                for (auto x = 0; x < 5; ++x) {
                    CHECK(fs::is_regular_file(base_dir / std::to_string(y) /
                                              std::to_string(x)));
                }
            }
            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        // cleanup
        if (fs::exists(base_dir)) {
            fs::remove_all(base_dir);
        }
        return retval;
    }

    acquire_export int unit_test__file_creator__create_shard_sinks()
    {
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });
            zarr::FileCreator file_creator{ thread_pool };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, 10, 2, 5); // 5 chunks, 1 shard
            dims.emplace_back(
              "y", DimensionType_Space, 4, 2, 1); // 2 chunks, 2 shards
            dims.emplace_back(
              "z", DimensionType_Space, 8, 2, 2); // 4 chunks, 2 shards

            std::vector<zarr::FilesystemSink*> files;
            CHECK(
              file_creator.create_shard_sinks(base_dir.string(), dims, files));

            CHECK(files.size() == 2);
            std::for_each(files.begin(),
                          files.end(),
                          [](zarr::FilesystemSink* f) { sink_close(f); });

            CHECK(fs::is_directory(base_dir));
            for (auto y = 0; y < 2; ++y) {
                CHECK(fs::is_directory(base_dir / std::to_string(y)));
                for (auto x = 0; x < 1; ++x) {
                    CHECK(fs::is_regular_file(base_dir / std::to_string(y) /
                                              std::to_string(x)));
                }
            }

            // cleanup
            fs::remove_all(base_dir);

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Exception: %s\n", exc.what());
        } catch (...) {
            LOGE("Exception: (unknown)");
        }

        // cleanup
        if (fs::exists(base_dir)) {
            fs::remove_all(base_dir);
        }
        return retval;
    }
} // extern "C"
#endif // NO_UNIT_TESTS