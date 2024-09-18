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

std::unique_ptr<zarr::Sink>
zarr::SinkCreator::make_sink(std::string_view base_uri, std::string_view path)
{
    bool is_s3 = common::is_web_uri(base_uri);
    std::string bucket_name;
    std::string base_dir;

    if (is_s3) {
        common::parse_path_from_uri(base_uri, bucket_name, base_dir);

        // create the bucket if it doesn't already exist
        if (!bucket_exists_(bucket_name)) {
            return nullptr;
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

        // create the parent directory if it doesn't already exist
        auto parent_path =
          fs::path(base_dir + "/" + std::string(path)).parent_path();

        if (!fs::is_directory(parent_path) &&
            !fs::create_directories(parent_path)) {
            return nullptr;
        }
    }

    const std::string full_path = base_dir + "/" + std::string(path);

    std::unique_ptr<Sink> sink;
    if (is_s3) {
        sink =
          std::make_unique<S3Sink>(bucket_name, full_path, connection_pool_);
    } else {
        sink = std::make_unique<FileSink>(full_path);
    }

    return sink;
}

bool
zarr::SinkCreator::make_data_sinks(
  const std::string& base_uri,
  const std::vector<Dimension>& dimensions,
  const std::function<size_t(const Dimension&)>& parts_along_dimension,
  std::vector<std::unique_ptr<Sink>>& part_sinks)
{
    std::queue<std::string> paths;

    bool is_s3 = common::is_web_uri(base_uri);
    std::string bucket_name;
    if (is_s3) {
        std::string base_dir;
        common::parse_path_from_uri(base_uri, bucket_name, base_dir);

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
zarr::SinkCreator::make_metadata_sinks(
  acquire::sink::zarr::ZarrVersion version,
  const std::string& base_uri,
  std::unordered_map<std::string, std::unique_ptr<Sink>>& metadata_sinks)
{
    EXPECT(!base_uri.empty(), "URI must not be empty.");

    std::vector<std::string> dir_paths, file_paths;

    switch (version) {
        case ZarrVersion::V2:
            dir_paths.emplace_back("0");

            file_paths.emplace_back(".zattrs");
            file_paths.emplace_back(".zgroup");
            file_paths.emplace_back("0/.zattrs");
            break;
        case ZarrVersion::V3:
            dir_paths.emplace_back("meta");
            dir_paths.emplace_back("meta/root");

            file_paths.emplace_back("zarr.json");
            file_paths.emplace_back("meta/root.group.json");
            break;
        default:
            throw std::runtime_error("Invalid Zarr version " +
                                     std::to_string(static_cast<int>(version)));
    }

    bool is_s3 = common::is_web_uri(base_uri);
    std::string bucket_name;
    std::string base_dir;

    if (is_s3) {
        common::parse_path_from_uri(base_uri, bucket_name, base_dir);

        // create the bucket if it doesn't already exist
        if (!bucket_exists_(bucket_name)) {
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
        if (!base_dir.empty() && !fs::is_directory(base_dir) &&
            !fs::create_directories(base_dir)) {
            return false;
        }

        const std::string prefix = base_dir.empty() ? "" : base_dir + "/";
        for (const auto& dir_path : dir_paths) {
            const auto dir = prefix + dir_path;
            if (!fs::is_directory(dir) && !fs::create_directories(dir)) {
                return false;
            }
        }
    }

    return is_s3 ? make_s3_objects_(bucket_name, file_paths, metadata_sinks)
                 : make_files_(base_dir, file_paths, metadata_sinks);
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
zarr::SinkCreator::make_files_(
  const std::string& base_dir,
  const std::vector<std::string>& file_paths,
  std::unordered_map<std::string, std::unique_ptr<Sink>>& sinks)
{
    if (file_paths.empty()) {
        return true;
    }

    std::atomic<char> all_successful = 1;

    const auto n_files = file_paths.size();
    std::latch latch(n_files);

    sinks.clear();
    for (const auto& filename : file_paths) {
        sinks[filename] = nullptr;
        std::unique_ptr<Sink>* psink = &sinks[filename];

        const std::string prefix = base_dir.empty() ? "" : base_dir + "/";
        const auto file_path = prefix + filename;

        thread_pool_->push_to_job_queue(
          [filename = file_path, psink, &latch, &all_successful](
            std::string& err) -> bool {
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
zarr::SinkCreator::bucket_exists_(std::string_view bucket_name)
{
    CHECK(!bucket_name.empty());
    EXPECT(connection_pool_, "S3 connection pool not provided.");

    auto conn = connection_pool_->get_connection();
    bool bucket_exists = conn->bucket_exists(bucket_name);

    connection_pool_->return_connection(std::move(conn));

    return bucket_exists;
}

bool
zarr::SinkCreator::make_s3_objects_(std::string_view bucket_name,
                                    std::queue<std::string>& object_keys,
                                    std::vector<std::unique_ptr<Sink>>& sinks)
{
    if (object_keys.empty()) {
        return true;
    }
    if (bucket_name.empty()) {
        LOGE("Bucket name not provided.");
        return false;
    }
    if (!connection_pool_) {
        LOGE("S3 connection pool not provided.");
        return false;
    }

    const auto n_objects = object_keys.size();
    sinks.resize(n_objects);
    for (auto i = 0; i < n_objects; ++i) {
        sinks[i] = std::make_unique<S3Sink>(
          bucket_name, object_keys.front(), connection_pool_);
        object_keys.pop();
    }

    return true;
}

bool
zarr::SinkCreator::make_s3_objects_(
  std::string_view bucket_name,
  std::vector<std::string>& object_keys,
  std::unordered_map<std::string, std::unique_ptr<Sink>>& sinks)
{
    if (object_keys.empty()) {
        return true;
    }
    if (bucket_name.empty()) {
        LOGE("Bucket name not provided.");
        return false;
    }
    if (!connection_pool_) {
        LOGE("S3 connection pool not provided.");
        return false;
    }

    sinks.clear();
    for (const auto& key : object_keys) {
        sinks[key] =
          std::make_unique<S3Sink>(bucket_name, key, connection_pool_);
    }

    return true;
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
