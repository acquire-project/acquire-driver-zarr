#ifndef H_ACQUIRE_STORAGE_ZARR_FILESYSTEM_SINK_V0
#define H_ACQUIRE_STORAGE_ZARR_FILESYSTEM_SINK_V0

#include "sink.hh"
#include "platform.h"

#include <filesystem>

namespace fs = std::filesystem;

namespace acquire::sink::zarr {
struct FileSink : public Sink
{
  public:
    explicit FileSink(const std::string& uri);
    ~FileSink() override;

    [[nodiscard]] bool write(size_t offset,
                             const uint8_t* buf,
                             size_t bytes_of_buf) override;

  private:
    struct file* file_;
};

struct FileCreator
{
  public:
    FileCreator() = delete;
    explicit FileCreator(std::shared_ptr<common::ThreadPool> thread_pool);
    ~FileCreator() noexcept = default;

    [[nodiscard]] bool create_chunk_sinks(
      const std::string& base_uri,
      const std::vector<Dimension>& dimensions,
      std::vector<Sink*>& chunk_sinks);

    [[nodiscard]] bool create_shard_sinks(
      const std::string& base_uri,
      const std::vector<Dimension>& dimensions,
      std::vector<Sink*>& shard_sinks);

    [[nodiscard]] bool create_metadata_sinks(
      const std::vector<std::string>& paths,
      std::vector<Sink*>& metadata_sinks);

  private:
    std::shared_ptr<common::ThreadPool> thread_pool_;

    /// @brief Parallel create a collection of directories.
    /// @param[in] dir_paths The directories to create.
    /// @return True iff all directories were created successfully.
    [[nodiscard]] bool make_dirs_(std::queue<fs::path>& dir_paths);

    /// @brief Parallel create a collection of files.
    /// @param[in,out] file_paths The files to create. Unlike `make_dirs_`,
    /// this function drains the queue.
    /// @param[out] files The files created.
    /// @return True iff all files were created successfully.
    [[nodiscard]] bool make_files_(std::queue<fs::path>& file_paths,
                                   std::vector<Sink*>& files);
};
} // namespace acquire::sink::zarr

#endif // H_ACQUIRE_STORAGE_ZARR_FILESYSTEM_SINK_V0
