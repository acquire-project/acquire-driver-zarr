#include "sink.creator.hh"
#include "file.sink.hh"
#include "s3.sink.hh"
#include "../common.hh"

#include <latch>
#include <queue>

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

zarr::SinkCreator::SinkCreator(
  std::shared_ptr<ThreadPool> thread_pool_,
  std::shared_ptr<S3ConnectionPool> connection_pool)
  : thread_pool_{ thread_pool_ }
  , connection_pool_{ connection_pool }
{
}

bool
zarr::SinkCreator::create_chunk_sinks(
  const std::string& base_uri,
  const std::vector<Dimension>& dimensions,
  std::vector<std::shared_ptr<Sink>>& chunk_sinks)
{
    std::string base_dir = base_uri;
    if (base_uri.starts_with("file://")) {
        base_dir = base_uri.substr(7);
    }

    std::queue<std::string> paths;
    paths.emplace(base_dir);

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
                paths.push(path + "/" + std::to_string(k));
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
                paths.push(path + "/" + std::to_string(j));
            }
        }
    }

    return make_files_(paths, chunk_sinks);
}

bool
zarr::SinkCreator::create_shard_sinks(
  const std::string& base_uri,
  const std::vector<Dimension>& dimensions,
  std::vector<std::shared_ptr<Sink>>& shard_sinks)
{
    std::string base_dir = base_uri;
    if (base_uri.starts_with("file://")) {
        base_dir = base_uri.substr(7);
    }

    std::queue<std::string> paths;
    paths.emplace(base_dir);

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
                paths.push(path + "/" + std::to_string(k));
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
                paths.push(path + "/" + std::to_string(j));
            }
        }
    }

    return make_files_(paths, shard_sinks);
}

bool
zarr::SinkCreator::create_v2_metadata_sinks(
  const std::string& base_uri,
  size_t n_arrays,
  std::vector<std::shared_ptr<Sink>>& metadata_sinks)
{
    std::string base_dir = base_uri;
    if (base_uri.starts_with("file://")) {
        base_dir = base_uri.substr(7);
    }

    std::queue<std::string> paths;
    paths.push(base_dir + "/.metadata"); // base metadata
    paths.push(base_dir + "/0/.zattrs"); // external metadata
    paths.push(base_dir + "/.zattrs");   // group metadata
    for (auto i = 0; i < n_arrays; ++i) {
        paths.push(base_dir + "/" + std::to_string(i) + "/.zarray");
    }

    // TODO (aliddell): or make_s3_objects_
    return make_files_(paths, metadata_sinks);
}

bool
zarr::SinkCreator::create_v3_metadata_sinks(
  const std::string& base_uri,
  size_t n_arrays,
  std::vector<std::shared_ptr<Sink>>& metadata_sinks)
{
    std::string base_dir = base_uri;
    if (base_uri.starts_with("file://")) {
        base_dir = base_uri.substr(7);
    }

    std::queue<std::string> paths;
    paths.push(base_dir + "/zarr.json");
    paths.push(base_dir + "/meta/root.group.json");
    for (auto i = 0; i < n_arrays; ++i) {
        paths.push(base_dir + "/meta/root/" + std::to_string(i) +
                   ".array.json");
    }

    // TODO (aliddell): or make_s3_objects_
    return make_files_(paths, metadata_sinks);
}

bool
zarr::SinkCreator::make_dirs_(std::queue<std::string>& dir_paths)
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
                           dirname.c_str(),
                           exc.what());
                  err = buf;
              } catch (...) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create directory '%s': (unknown).",
                           dirname.c_str());
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
zarr::SinkCreator::make_files_(std::queue<std::string>& file_paths,
                               std::vector<std::shared_ptr<Sink>>& sinks)
{
    if (file_paths.empty()) {
        return true;
    }

    std::atomic<bool> all_successful = true;

    const auto n_files = file_paths.size();
    sinks.resize(n_files);
    std::fill(sinks.begin(), sinks.end(), nullptr);
    std::latch latch(n_files);

    for (auto i = 0; i < n_files; ++i) {
        const auto filename = file_paths.front();
        file_paths.pop();

        std::shared_ptr<Sink>* psink = sinks.data() + i;

        thread_pool_->push_to_job_queue(
          [filename, psink, &latch, &all_successful](std::string& err) -> bool {
              bool success = false;

              try {
                  if (all_successful) {
                      *psink = std::make_shared<FileSink>(filename);
                  }
                  success = true;
              } catch (const std::exception& exc) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create file '%s': %s.",
                           filename.c_str(),
                           exc.what());
                  err = buf;
              } catch (...) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create file '%s': (unknown).",
                           filename.c_str());
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

bool
zarr::SinkCreator::make_s3_objects_(const std::string& bucket_name,
                                    std::queue<std::string>& object_keys,
                                    std::vector<std::shared_ptr<Sink>>& sinks)
{
    if (object_keys.empty()) {
        return true;
    }
    if (!connection_pool_) {
        LOGE("S3 connection pool not provided.");
        return false;
    }

    std::atomic<bool> all_successful = true;

    const auto n_objects = object_keys.size();
    sinks.resize(n_objects);
    std::fill(sinks.begin(), sinks.end(), nullptr);
    std::latch latch(n_objects);

    for (auto i = 0; i < n_objects; ++i) {
        const auto object_key = object_keys.front();
        object_keys.pop();

        std::shared_ptr<Sink>* psink = sinks.data() + i;

        thread_pool_->push_to_job_queue(
          [this, bucket_name, object_key, psink, &latch, &all_successful](
            std::string& err) -> bool {
              bool success = false;

              try {
                  if (all_successful) {
                      *psink = std::make_shared<S3Sink>(
                        bucket_name, object_key, connection_pool_);
                  }
                  success = true;
              } catch (const std::exception& exc) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create S3 object '%s': %s.",
                           object_key.c_str(),
                           exc.what());
                  err = buf;
              } catch (...) {
                  char buf[128];
                  snprintf(buf,
                           sizeof(buf),
                           "Failed to create S3 object '%s': (unknown).",
                           object_key.c_str());
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

#include "../common/thread.pool.hh"

extern "C"
{
    acquire_export int unit_test__sink_creator__create_chunk_file_sinks()
    {
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<zarr::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s\n", err.c_str()); });
            zarr::SinkCreator creator{ thread_pool, nullptr };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x", DimensionType_Space, 10, 2, 0); // 5 chunks
            dims.emplace_back("y", DimensionType_Space, 4, 2, 0);  // 2 chunks
            dims.emplace_back(
              "z", DimensionType_Space, 0, 3, 0); // 3 timepoints per chunk

            std::vector<std::shared_ptr<zarr::Sink>> files;
            CHECK(creator.create_chunk_sinks(base_dir.string(), dims, files));

            CHECK(files.size() == 5 * 2);
            for (const auto& f : files) {
                CHECK(f);
                f->close();
            }

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

    acquire_export int unit_test__sink_creator__create_shard_file_sinks()
    {
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<zarr::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s", err.c_str()); });
            zarr::SinkCreator creator{ thread_pool, nullptr };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back(
              "x", DimensionType_Space, 10, 2, 5); // 5 chunks, 1 shard
            dims.emplace_back(
              "y", DimensionType_Space, 4, 2, 1); // 2 chunks, 2 shards
            dims.emplace_back(
              "z", DimensionType_Space, 8, 2, 2); // 4 chunks, 2 shards

            std::vector<std::shared_ptr<zarr::Sink>> files;
            CHECK(creator.create_shard_sinks(base_dir.string(), dims, files));

            CHECK(files.size() == 2);
            for (auto& f : files) {
                CHECK(f);
                f->close();
            }

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