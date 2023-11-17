#include "common.hh"
#include "zarr.hh"

#include "platform.h"

#include <cmath>
#include <thread>

namespace common = acquire::sink::zarr::common;
common::ThreadPool::ThreadPool(size_t n_threads,
                               std::function<void(const std::string&)> err)
  : error_handler_{ err }
  , should_stop_{ false }
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
    await_stop();
}

void
common::ThreadPool::push_to_job_queue(JobT&& job)
{
    std::unique_lock lock(jobs_mutex_);
    jobs_.push(std::move(job));
    lock.unlock();

    cv_.notify_one();
}

void
common::ThreadPool::await_stop() noexcept
{
    should_stop_ = true;
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

void
common::ThreadPool::thread_worker_()
{
    TRACE("Worker thread starting.");

    while (true) {
        std::unique_lock lock(jobs_mutex_);
        cv_.wait(lock, [&] { return should_stop_ || !jobs_.empty(); });

        if (should_stop_) {
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
common::bytes_per_tile(const ImageDims& tile_shape, const SampleType& type)
{
    return bytes_of_type(type) * tile_shape.rows * tile_shape.cols;
}

size_t
common::frames_per_chunk(const ImageDims& tile_shape,
                         SampleType type,
                         uint64_t max_bytes_per_chunk)
{
    auto bpt = (float)bytes_per_tile(tile_shape, type);
    if (0 == bpt)
        return 0;

    return (size_t)std::floor((float)max_bytes_per_chunk / bpt);
}

size_t
common::bytes_per_chunk(const ImageDims& tile_shape,
                        const SampleType& type,
                        uint64_t max_bytes_per_chunk)
{
    return bytes_per_tile(tile_shape, type) *
           frames_per_chunk(tile_shape, type, max_bytes_per_chunk);
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
