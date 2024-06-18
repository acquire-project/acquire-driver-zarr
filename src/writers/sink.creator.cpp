#include "sink.creator.hh"
#include "file.sink.hh"

#include <latch>
#include <queue>

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

zarr::SinkCreator::SinkCreator(std::shared_ptr<common::ThreadPool> thread_pool_)
  : thread_pool_{ thread_pool_ }
{
}

bool
zarr::SinkCreator::create_chunk_sinks(
  const std::string& base_uri,
  const std::vector<Dimension>& dimensions,
  std::vector<std::shared_ptr<Sink>>& chunk_sinks)
{
    return make_part_sinks_(
      base_uri, dimensions, common::chunks_along_dimension, chunk_sinks);
}

bool
zarr::SinkCreator::create_shard_sinks(
  const std::string& base_uri,
  const std::vector<Dimension>& dimensions,
  std::vector<std::shared_ptr<Sink>>& shard_sinks)
{
    return make_part_sinks_(
      base_uri, dimensions, common::shards_along_dimension, shard_sinks);
}

bool
zarr::SinkCreator::create_v2_metadata_sinks(
  const std::string& base_uri,
  size_t n_arrays,
  std::vector<std::shared_ptr<Sink>>& metadata_sinks)
{
    std::queue<std::string> dir_paths, file_paths;

    file_paths.emplace(".metadata"); // base metadata
    file_paths.emplace("0/.zattrs"); // external metadata
    file_paths.emplace(".zattrs");   // group metadata

    for (auto i = 0; i < n_arrays; ++i) {
        dir_paths.push(std::to_string(i));
        file_paths.push(std::to_string(i) + "/.zarray"); // array metadata
    }

    return make_metadata_sinks_(
      base_uri, dir_paths, file_paths, metadata_sinks);
}

bool
zarr::SinkCreator::create_v3_metadata_sinks(
  const std::string& base_uri,
  size_t n_arrays,
  std::vector<std::shared_ptr<Sink>>& metadata_sinks)
{
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
                               std::vector<std::shared_ptr<Sink>>& sinks)
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
              all_successful.fetch_and((char)success);

              return success;
          });
    }

    latch.wait();

    return (bool)all_successful;
}

bool
zarr::SinkCreator::make_part_sinks_(
  const std::string& base_uri,
  const std::vector<Dimension>& dimensions,
  const std::function<size_t(const Dimension&)>& parts_along_dimension,
  std::vector<std::shared_ptr<Sink>>& part_sinks)
{
    std::queue<std::string> paths;

    std::string base_dir = base_uri;
    if (base_uri.starts_with("file://")) {
        base_dir = base_uri.substr(7);
    }
    paths.emplace(base_dir);

    if (!make_dirs_(paths)) {
        return false;
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

        if (!make_dirs_(paths)) {
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

    return make_files_(paths, part_sinks);
}

bool
zarr::SinkCreator::make_metadata_sinks_(
  const std::string& base_uri,
  std::queue<std::string>& dir_paths,
  std::queue<std::string>& file_paths,
  std::vector<std::shared_ptr<Sink>>& metadata_sinks)
{
    std::string base_dir = base_uri;
    if (base_uri.starts_with("file://")) {
        base_dir = base_uri.substr(7);
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

    // make files
    size_t n_paths = file_paths.size();
    for (auto i = 0; i < n_paths; ++i) {
        const auto path = base_dir + "/" + file_paths.front();
        file_paths.pop();
        file_paths.push(path);
    }

    if (!make_files_(file_paths, metadata_sinks)) {
        return false;
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
            zarr::SinkCreator creator{ thread_pool };

            std::vector<zarr::Dimension> dims;
            dims.emplace_back("x", DimensionType_Space, 10, 2, 0); // 5 chunks
            dims.emplace_back("y", DimensionType_Space, 4, 2, 0);  // 2 chunks
            dims.emplace_back(
              "z", DimensionType_Space, 0, 3, 0); // 3 timepoints per chunk

            std::vector<std::shared_ptr<zarr::Sink>> files;
            CHECK(creator.create_chunk_sinks(base_dir.string(), dims, files));

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
            zarr::SinkCreator creator{ thread_pool };

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
