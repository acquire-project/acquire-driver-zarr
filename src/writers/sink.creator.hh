#ifndef H_ACQUIRE_STORAGE_ZARR_SINK_CREATOR_V0
#define H_ACQUIRE_STORAGE_ZARR_SINK_CREATOR_V0

#include "sink.hh"
#include "../common/thread.pool.hh"
#include "../common/connection.pool.hh"

#include <optional>

namespace acquire::sink::zarr {
struct SinkCreator
{
  public:
    SinkCreator() = delete;
    SinkCreator(std::shared_ptr<ThreadPool> thread_pool,
                std::shared_ptr<S3ConnectionPool> connection_pool);
    ~SinkCreator() noexcept = default;

    [[nodiscard]] bool create_chunk_sinks(
      const std::string& base_uri,
      const std::vector<Dimension>& dimensions,
      std::vector<std::shared_ptr<Sink>>& chunk_sinks);

    [[nodiscard]] bool create_shard_sinks(
      const std::string& base_uri,
      const std::vector<Dimension>& dimensions,
      std::vector<std::shared_ptr<Sink>>& shard_sinks);

    [[nodiscard]] bool create_v2_metadata_sinks(
      const std::string& base_uri,
      size_t n_arrays,
      std::vector<std::shared_ptr<Sink>>& metadata_sinks);
    [[nodiscard]] bool create_v3_metadata_sinks(
      const std::string& base_uri,
      size_t n_arrays,
      std::vector<std::shared_ptr<Sink>>& metadata_sinks);

  private:
    std::shared_ptr<ThreadPool> thread_pool_;
    std::shared_ptr<S3ConnectionPool> connection_pool_; // could be null

    /// @brief Parallel create a collection of directories.
    /// @param[in] dir_paths The directories to create.
    /// @return True iff all directories were created successfully.
    [[nodiscard]] bool make_dirs_(std::queue<std::string>& dir_paths);

    /// @brief Parallel create a collection of files.
    /// @param[in,out] file_paths The files to create. Unlike `make_dirs_`,
    /// this function drains the queue.
    /// @param[out] files The files created.
    /// @return True iff all files were created successfully.
    [[nodiscard]] bool make_files_(std::queue<std::string>& file_paths,
                                   std::vector<std::shared_ptr<Sink>>& sinks);

    [[nodiscard]] bool make_s3_objects_(
      const std::string& bucket_name,
      std::queue<std::string>& object_keys,
      std::vector<std::shared_ptr<Sink>>& sinks);
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_SINK_CREATOR_V0
