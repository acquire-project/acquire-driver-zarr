#include "sink.creator.hh"
#include "file.sink.hh"
#include "s3.sink.hh"
#include "common/utilities.hh"

#include <latch>
#include <queue>

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

zarr::SinkCreator::SinkCreator(
  std::shared_ptr<common::ThreadPool> thread_pool_,
  std::shared_ptr<common::S3ConnectionPool> connection_pool)
  : thread_pool_{ thread_pool_ }
  , connection_pool_{ connection_pool }
{
}

bool
zarr::SinkCreator::make_data_sinks(
  const std::string& base_uri,
  const std::vector<Dimension>& dimensions,
  const std::function<size_t(const Dimension&)>& parts_along_dimension,
  std::vector<std::unique_ptr<Sink>>& part_sinks)
{
    std::queue<std::string> paths;

    bool is_s3 = common::is_s3_uri(base_uri);
    std::string bucket_name;
    if (is_s3) {
        auto tokens = common::split_uri(base_uri);
        CHECK(tokens.size() > 2);
        bucket_name = tokens.at(2);

        // create the bucket if it doesn't already exist
        if (!make_s3_bucket_(bucket_name)) {
            return false;
        }

        std::string base_dir;
        for (auto i = 3; i < tokens.size() - 1; ++i) {
            base_dir += tokens.at(i) + "/";
        }
        if (tokens.size() > 3) {
            base_dir += tokens.at(tokens.size() - 1);
        }

        paths.push(base_dir);
    } else {
        std::string base_dir = base_uri;

        if (base_uri.starts_with("file://")) {
            base_dir = base_uri.substr(7);
        }
        paths.emplace(base_dir);

        if (!make_dirs_(paths)) {
            return false;
        }
    }

    // create directories
    for (auto i = dimensions.size() - 2; i >= 1; --i) {
        const auto& dim = dimensions.at(i);
        const auto n_parts = parts_along_dimension(dim);
        CHECK(n_parts);

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_parts; ++k) {
                const auto kstr = std::to_string(k);
                paths.push(path + (path.empty() ? kstr : "/" + kstr));
            }
        }

        if (!is_s3 && !make_dirs_(paths)) {
            return false;
        }
    }

    // create files
    {
        const auto& dim = dimensions.front();
        const auto n_parts = parts_along_dimension(dim);
        CHECK(n_parts);

        auto n_paths = paths.size();
        for (auto i = 0; i < n_paths; ++i) {
            const auto path = paths.front();
            paths.pop();
            for (auto j = 0; j < n_parts; ++j) {
                paths.push(path + "/" + std::to_string(j));
            }
        }
    }

    return is_s3 ? make_s3_objects_(bucket_name, paths, part_sinks)
                 : make_files_(paths, part_sinks);
}

bool
zarr::SinkCreator::create_v2_metadata_sinks(
  const std::string& base_uri,
  size_t n_arrays,
  std::vector<std::unique_ptr<Sink>>& metadata_sinks)
{
    if (base_uri.empty()) {
        LOGE("Base URI is empty.");
        return false;
    }

    std::queue<std::string> dir_paths, file_paths;

    file_paths.emplace(".metadata"); // base metadata
    file_paths.emplace("0/.zattrs"); // external metadata
    file_paths.emplace(".zattrs");   // group metadata

    for (auto i = 0; i < n_arrays; ++i) {
        const auto idx_string = std::to_string(i);
        dir_paths.push(idx_string);
        file_paths.push(idx_string + "/.zarray"); // array metadata
    }

    return make_metadata_sinks_(
      base_uri, dir_paths, file_paths, metadata_sinks);
}

bool
zarr::SinkCreator::create_v3_metadata_sinks(
  const std::string& base_uri,
  size_t n_arrays,
  std::vector<std::unique_ptr<Sink>>& metadata_sinks)
{
    if (base_uri.empty()) {
        LOGE("Base URI is empty.");
        return false;
    }

    std::queue<std::string> dir_paths, file_paths;

    dir_paths.emplace("meta");
    dir_paths.emplace("meta/root");

    file_paths.emplace("zarr.json");
    file_paths.emplace("meta/root.group.json");
    for (auto i = 0; i < n_arrays; ++i) {
        file_paths.push("meta/root/" + std::to_string(i) + ".array.json");
    }

    return make_metadata_sinks_(
      base_uri, dir_paths, file_paths, metadata_sinks);
}

bool
zarr::SinkCreator::make_dirs_(std::queue<std::string>& dir_paths)
{
    if (dir_paths.empty()) {
        return true;
    }

    std::atomic<char> all_successful = 1;

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
                  err = "Failed to create directory '" + dirname +
                        "': " + exc.what();
              } catch (...) {
                  err =
                    "Failed to create directory '" + dirname + "': (unknown).";
              }

              latch.count_down();
              all_successful.fetch_and((char)success);

              return success;
          });

        dir_paths.push(dirname);
    }

    latch.wait();

    return (bool)all_successful;
}

bool
zarr::SinkCreator::make_files_(std::queue<std::string>& file_paths,
                               std::vector<std::unique_ptr<Sink>>& sinks)
{
    if (file_paths.empty()) {
        return true;
    }

    std::atomic<char> all_successful = 1;

    const auto n_files = file_paths.size();
    sinks.resize(n_files);
    std::fill(sinks.begin(), sinks.end(), nullptr);
    std::latch latch(n_files);

    for (auto i = 0; i < n_files; ++i) {
        const auto filename = file_paths.front();
        file_paths.pop();

        std::unique_ptr<Sink>* psink = sinks.data() + i;

        thread_pool_->push_to_job_queue(
          [filename, psink, &latch, &all_successful](std::string& err) -> bool {
              bool success = false;

              try {
                  if (all_successful) {
                      *psink = std::make_unique<FileSink>(filename);
                  }
                  success = true;
              } catch (const std::exception& exc) {
                  err =
                    "Failed to create file '" + filename + "': " + exc.what();
              } catch (...) {
                  err = "Failed to create file '" + filename + "': (unknown).";
              }

              latch.count_down();
              all_successful.fetch_and((char)success);

              return success;
          });
    }

    latch.wait();

    return (bool)all_successful;
}

bool
zarr::SinkCreator::make_metadata_sinks_(
  const std::string& base_uri,
  std::queue<std::string>& dir_paths,
  std::queue<std::string>& file_paths,
  std::vector<std::unique_ptr<Sink>>& metadata_sinks)
{
    bool is_s3 = common::is_s3_uri(base_uri);
    std::string bucket_name;
    std::string base_dir;

    if (is_s3) {
        auto tokens = common::split_uri(base_uri);
        CHECK(tokens.size() > 2);
        bucket_name = tokens.at(2);

        for (auto i = 3; i < tokens.size() - 1; ++i) {
            base_dir += tokens.at(i) + "/";
        }
        if (tokens.size() > 3) {
            base_dir += tokens.at(tokens.size() - 1);
        }

        // create the bucket if it doesn't already exist
        if (!make_s3_bucket_(bucket_name)) {
            return false;
        }
    } else {
        base_dir = base_uri;
        if (base_uri.starts_with("file://")) {
            base_dir = base_uri.substr(7);
        }

        // remove trailing slashes
        if (base_uri.ends_with("/") || base_uri.ends_with("\\")) {
            base_dir = base_dir.substr(0, base_dir.size() - 1);
        }

        // create the base directories if they don't already exist
        // we create them in serial because
        // 1. there are only a few of them; and
        // 2. they may be nested
        while (!dir_paths.empty()) {
            const auto dir = base_dir + "/" + dir_paths.front();
            dir_paths.pop();
            if (!fs::is_directory(dir) && !fs::create_directories(dir)) {
                return false;
            }
        }
    }

    // make files
    size_t n_paths = file_paths.size();
    const std::string prefix = base_dir.empty() ? "" : base_dir + "/";
    for (auto i = 0; i < n_paths; ++i) {
        const auto path = prefix + file_paths.front();
        file_paths.pop();
        file_paths.push(path);
    }

    return is_s3 ? make_s3_objects_(bucket_name, file_paths, metadata_sinks)
                 : make_files_(file_paths, metadata_sinks);
}

bool
zarr::SinkCreator::make_s3_bucket_(const std::string& bucket_name)
{
    if (bucket_name.empty()) {
        return false;
    }

    bool retval = false;
    std::unique_ptr<common::S3Connection> conn;
    try {
        EXPECT(connection_pool_, "S3 connection pool not provided.");
        CHECK(conn = connection_pool_->get_connection());
        retval = conn->make_bucket(bucket_name);
    } catch (const std::exception& exc) {
        LOGE("Failed to create S3 bucket '%s': %s",
             bucket_name.c_str(),
             exc.what());
    } catch (...) {
        LOGE("Failed to create S3 bucket '%s': (unknown)", bucket_name.c_str());
    }

    if (conn) {
        connection_pool_->release_connection(std::move(conn));
    }

    return retval;
}
bool
zarr::SinkCreator::make_s3_objects_(const std::string& bucket_name,
                                    std::queue<std::string>& object_keys,
                                    std::vector<std::unique_ptr<Sink>>& sinks)
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

        std::unique_ptr<Sink>* psink = sinks.data() + i;

        thread_pool_->push_to_job_queue(
          [this, bucket_name, object_key, psink, &latch, &all_successful](
            std::string& err) -> bool {
              bool success = false;

              try {
                  if (all_successful) {
                      *psink = std::make_unique<S3Sink>(
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

extern "C"
{
    acquire_export int unit_test__sink_creator__create_chunk_file_sinks()
    {
        const fs::path base_dir = fs::temp_directory_path() / "acquire";
        int retval = 0;

        try {
            auto thread_pool = std::make_shared<common::ThreadPool>(
              std::thread::hardware_concurrency(),
              [](const std::string& err) { LOGE("Error: %s\n", err.c_str()); });
            zarr::SinkCreator creator{ thread_pool, nullptr };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x", DimensionType_Space, 10, 2, 0); // 5 chunks
            dims.emplace_back("y", DimensionType_Space, 4, 2, 0);  // 2 chunks
            dims.emplace_back(
              "z", DimensionType_Space, 0, 3, 0); // 3 timepoints per chunk

            std::vector<std::unique_ptr<zarr::Sink>> files;
            CHECK(creator.make_data_sinks(
              base_dir.string(), dims, common::chunks_along_dimension, files));

            CHECK(files.size() == 5 * 2);
            files.clear(); // closes files

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
            auto thread_pool = std::make_shared<common::ThreadPool>(
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

            std::vector<std::unique_ptr<zarr::Sink>> files;
            CHECK(creator.make_data_sinks(
              base_dir.string(), dims, common::shards_along_dimension, files));

            CHECK(files.size() == 2);
            files.clear(); // closes files

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
