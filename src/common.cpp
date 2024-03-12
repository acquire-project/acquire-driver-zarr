#include "common.hh"
#include "zarr.hh"

#include "platform.h"

#include <cmath>
#include <thread>

namespace zarr = acquire::sink::zarr;
namespace common = zarr::common;

zarr::Dimension::Dimension(const std::string& name,
                           DimensionType kind,
                           uint32_t array_size_px,
                           uint32_t chunk_size_px,
                           uint32_t shard_size_chunks)
  : name{ name }
  , kind{ kind }
  , array_size_px{ array_size_px }
  , chunk_size_px{ chunk_size_px }
  , shard_size_chunks{ shard_size_chunks }
{
    EXPECT(kind < DimensionTypeCount, "Invalid dimension type.");
    EXPECT(!name.empty(), "Dimension name cannot be empty.");
}

zarr::Dimension::Dimension(const StorageDimension& dim)
  : Dimension(dim.name.str,
              dim.kind,
              dim.array_size_px,
              dim.chunk_size_px,
              dim.shard_size_chunks)
{
}

common::ThreadPool::ThreadPool(size_t n_threads,
                               std::function<void(const std::string&)> err)
  : error_handler_{ err }
  , is_accepting_jobs_{ true }
{
    n_threads = std::clamp(
      n_threads,
      (size_t)1,
      (size_t)std::max(std::thread::hardware_concurrency(), (unsigned)1));

    for (auto i = 0; i < n_threads; ++i) {
        threads_.emplace_back([this] { thread_worker_(); });
    }
}

common::ThreadPool::~ThreadPool() noexcept
{
    {
        std::scoped_lock lock(jobs_mutex_);
        while (!jobs_.empty()) {
            jobs_.pop();
        }
    }

    await_stop();
}

void
common::ThreadPool::push_to_job_queue(JobT&& job)
{
    std::unique_lock lock(jobs_mutex_);
    CHECK(is_accepting_jobs_);

    jobs_.push(std::move(job));
    lock.unlock();

    cv_.notify_one();
}

void
common::ThreadPool::await_stop() noexcept
{
    {
        std::scoped_lock lock(jobs_mutex_);
        is_accepting_jobs_ = false;
    }

    cv_.notify_all();

    // spin down threads
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

std::optional<common::ThreadPool::JobT>
common::ThreadPool::pop_from_job_queue_() noexcept
{
    if (jobs_.empty()) {
        return std::nullopt;
    }

    auto job = std::move(jobs_.front());
    jobs_.pop();
    return job;
}

bool
common::ThreadPool::should_stop_() const noexcept
{
    return !is_accepting_jobs_ && jobs_.empty();
}

void
common::ThreadPool::thread_worker_()
{
    TRACE("Worker thread starting.");

    while (true) {
        std::unique_lock lock(jobs_mutex_);
        cv_.wait(lock, [&] { return should_stop_() || !jobs_.empty(); });

        if (should_stop_()) {
            break;
        }

        if (auto job = pop_from_job_queue_(); job.has_value()) {
            lock.unlock();
            if (std::string err_msg; !job.value()(err_msg)) {
                error_handler_(err_msg);
            }
        }
    }

    TRACE("Worker thread exiting.");
}

size_t
common::chunks_along_dimension(const Dimension& dimension)
{
    EXPECT(dimension.chunk_size_px > 0, "Invalid chunk_size size.");

    return (dimension.array_size_px + dimension.chunk_size_px - 1) /
           dimension.chunk_size_px;
}

size_t
common::shards_along_dimension(const Dimension& dimension)
{
    if (dimension.shard_size_chunks == 0) {
        return 0;
    }

    // shard_size_chunks evenly divides the number of chunks
    return chunks_along_dimension(dimension) / dimension.shard_size_chunks;
}

size_t
common::number_of_chunks_in_memory(const std::vector<Dimension>& dimensions)
{
    size_t n_chunks = 1;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        n_chunks *= chunks_along_dimension(dimensions[i]);
    }

    return n_chunks;
}

size_t
common::number_of_shards(const std::vector<Dimension>& dimensions)
{
    size_t n_shards = 1;
    for (auto i = 0; i < dimensions.size() - 1; ++i) {
        const auto& dim = dimensions.at(i);
        n_shards *= shards_along_dimension(dim);
    }

    return n_shards;
}

size_t
common::chunks_per_shard(const std::vector<Dimension>& dimensions)
{
    size_t n_chunks = 1;
    for (const auto& dim : dimensions) {
        n_chunks *= dim.shard_size_chunks;
    }

    return n_chunks;
}

size_t
common::bytes_per_chunk(const std::vector<Dimension>& dimensions,
                        const SampleType& type)
{
    auto n_bytes = bytes_of_type(type);
    for (const auto& d : dimensions) {
        n_bytes *= d.chunk_size_px;
    }

    return n_bytes;
}

const char*
common::sample_type_to_dtype(SampleType t)

{
    static const char* table[] = { "u1", "u2", "i1", "i2",
                                   "f4", "u2", "u2", "u2" };
    if (t < countof(table)) {
        return table[t];
    } else {
        throw std::runtime_error("Invalid sample type.");
    }
}

const char*
common::sample_type_to_string(SampleType t) noexcept
{
    static const char* table[] = { "u8",  "u16", "i8",  "i16",
                                   "f32", "u16", "u16", "u16" };
    if (t < countof(table)) {
        return table[t];
    } else {
        return "unrecognized pixel type";
    }
}

void
common::write_string(const std::string& path, const std::string& str)
{
    if (auto p = fs::path(path); !fs::exists(p.parent_path()))
        fs::create_directories(p.parent_path());

    struct file f = { 0 };
    auto is_ok = file_create(&f, path.c_str(), path.size());
    is_ok &= file_write(&f,                                  // file
                        0,                                   // offset
                        (uint8_t*)str.c_str(),               // cur
                        (uint8_t*)(str.c_str() + str.size()) // end
    );
    EXPECT(is_ok, "Write to \"%s\" failed.", path.c_str());
    TRACE("Wrote %d bytes to \"%s\".", str.size(), path.c_str());
    file_close(&f);
}
