#pragma once

#include <cstddef> // size_t
#include <memory>  // unique_ptr

#include "stream.settings.hh"
#include "thread.pool.hh"

struct ZarrStream_s
{
  public:
    ZarrStream_s(struct ZarrStreamSettings_s* settings, size_t version);
    ~ZarrStream_s();

  private:
    struct ZarrStreamSettings_s settings_;
    size_t version_;

    std::unique_ptr<zarr::ThreadPool> thread_pool_;

    std::string error_; // error message. If nonempty, an error occurred.

    /**
     * @brief Set an error message.
     * @param msg The error message to set.
     */
    void set_error_(const std::string& msg);

    void create_store_();
};
