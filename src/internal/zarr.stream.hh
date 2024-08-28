#pragma once

#include "stream.settings.hh"
#include "thread.pool.hh"
#include "s3.connection.hh"
#include "array.writer.hh"

#include <cstddef> // size_t
#include <memory>  // unique_ptr

struct ZarrStream_s
{
  public:
    ZarrStream_s(struct ZarrStreamSettings_s* settings, size_t version);
    ~ZarrStream_s();

    size_t version() const { return version_; }
    ZarrStreamSettings_s& settings() { return settings_; }

  private:
    struct ZarrStreamSettings_s settings_;
    size_t version_; // Zarr version. 2 or 3.
    std::string error_; // error message. If nonempty, an error occurred.

    std::shared_ptr<zarr::ThreadPool> thread_pool_;
    std::shared_ptr<zarr::S3ConnectionPool> s3_connection_pool_;

    std::vector<std::unique_ptr<zarr::ArrayWriter>> writers_;

    /**
     * @brief Set an error message.
     * @param msg The error message to set.
     */
    void set_error_(const std::string& msg);

    /** @brief Create the data store. */
    [[nodiscard]] bool create_store_();
    void create_filesystem_store_();
    void create_s3_connection_pool_();

    /** @brief Create the writers. */
    [[nodiscard]] bool create_writers_();
};
