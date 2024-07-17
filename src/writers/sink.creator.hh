#ifndef H_ACQUIRE_STORAGE_ZARR_WRITERS_SINK_CREATOR_V0
#define H_ACQUIRE_STORAGE_ZARR_WRITERS_SINK_CREATOR_V0

#include "sink.hh"
#include "common/dimension.hh"
#include "common/thread.pool.hh"
#include "common/s3.connection.hh"

#include <optional>
#include <memory>

namespace acquire::sink::zarr {
struct SinkCreator final
{
  public:
    SinkCreator() = delete;
    SinkCreator(std::shared_ptr<common::ThreadPool> thread_pool_,
                std::shared_ptr<common::S3ConnectionPool> connection_pool);
    ~SinkCreator() noexcept = default;

    /// @brief Create a collection of data sinks, either chunk or shard.
    /// @param[in] base_uri The base URI for the sinks.
    /// @param[in] dimensions The dimensions of the data.
    /// @param[in] parts_along_dimension Function for computing the number of
    ///            parts (either chunk or shard) along each dimension.
    /// @param[out] part_sinks The sinks created.
    [[nodiscard]] bool make_data_sinks(
      const std::string& base_uri,
      const std::vector<Dimension>& dimensions,
      const std::function<size_t(const Dimension&)>& parts_along_dimension,
      std::vector<std::unique_ptr<Sink>>& part_sinks);

    /// @brief Create a collection of metadata sinks for a Zarr V2 dataset.
    /// @param[in] base_uri The base URI for the dataset.
    /// @param[in] n_arrays The number of data arrays.
    /// @param[out] metadata_sinks The sinks created.
    [[nodiscard]] bool create_v2_metadata_sinks(
      const std::string& base_uri,
      size_t n_arrays,
      std::vector<std::unique_ptr<Sink>>& metadata_sinks);

    /// @brief Create a collection of metadata sinks for a Zarr V3 dataset.
    /// @param[in] base_uri The base URI for the dataset.
    /// @param[in] n_arrays The number of data arrays.
    /// @param[out] metadata_sinks The sinks created.
    [[nodiscard]] bool create_v3_metadata_sinks(
      const std::string& base_uri,
      size_t n_arrays,
      std::vector<std::unique_ptr<Sink>>& metadata_sinks);

  private:
    std::shared_ptr<common::ThreadPool> thread_pool_;
    std::shared_ptr<common::S3ConnectionPool> connection_pool_; // could be null

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
                                   std::vector<std::unique_ptr<Sink>>& sinks);

    /// @brief Create a collection of metadata sinks.
    /// @param[in] base_uri The base URI for the sinks.
    /// @param[in] dir_paths The directories to create.
    /// @param[in] file_paths The files to create.
    /// @param[out] metadata_sinks The sinks created.
    [[nodiscard]] bool make_metadata_sinks_(
      const std::string& base_uri,
      std::queue<std::string>& dir_paths,
      std::queue<std::string>& file_paths,
      std::vector<std::unique_ptr<Sink>>& metadata_sinks);
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_WRITERS_SINK_CREATOR_V0