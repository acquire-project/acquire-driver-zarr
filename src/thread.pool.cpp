#include "thread.pool.hh"
#include "logger.h"

#define LOG(...) aq_logger(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOGE(...) aq_logger(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            LOGE(__VA_ARGS__);                                                 \
            throw std::runtime_error("Expression was false: " #e);             \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t%s", #e)

// #define TRACE(...) LOG(__VA_ARGS__)
#define TRACE(...)

#define containerof(ptr, T, V) ((T*)(((char*)(ptr)) - offsetof(T, V)))
#define countof(e) (sizeof(e) / sizeof(*(e)))

namespace zarr = acquire::sink::zarr;

zarr::ThreadPool::ThreadPool(size_t n_threads,
                             std::function<void(const std::string&)>&& err)
  : error_handler_{ std::move(err) }
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

zarr::ThreadPool::~ThreadPool() noexcept
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
zarr::ThreadPool::push_to_job_queue(JobT&& job)
{
    std::unique_lock lock(jobs_mutex_);
    CHECK(is_accepting_jobs_);

    jobs_.push(std::move(job));
    lock.unlock();

    cv_.notify_one();
}

void
zarr::ThreadPool::await_stop() noexcept
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

std::optional<zarr::ThreadPool::JobT>
zarr::ThreadPool::pop_from_job_queue_() noexcept
{
    if (jobs_.empty()) {
        return std::nullopt;
    }

    auto job = std::move(jobs_.front());
    jobs_.pop();
    return job;
}

bool
zarr::ThreadPool::should_stop_() const noexcept
{
    return !is_accepting_jobs_ && jobs_.empty();
}

void
zarr::ThreadPool::thread_worker_()
{
    TRACE("Worker thread starting.");

    while (true) {
        std::unique_lock lock(jobs_mutex_);
        cv_.wait(lock, [&] { return should_stop_() || !jobs_.empty(); });

        if (should_stop_()) {
            break;
        }

        if (auto job = pop_from_job_queue_(); job) {
            lock.unlock();
            if (std::string err_msg; !(*job)(err_msg)) {
                error_handler_(err_msg);
            }
        }
    }

    TRACE("Worker thread exiting.");
}

#ifndef NO_UNIT_TESTS
#ifdef _WIN32
#define acquire_export __declspec(dllexport)
#else
#define acquire_export
#endif // _WIN32

#include <filesystem>
#include <fstream>
#include <iostream>

namespace fs = std::filesystem;

extern "C"
{
    acquire_export int unit_test__thread_pool__push_to_job_queue()
    {
        int retval = 0;

        fs::path tmp_path = fs::temp_directory_path() / "tmp_file";

        try {
            CHECK(!fs::exists(tmp_path));

            zarr::ThreadPool pool{ 1, [](const std::string&) {} };
            pool.push_to_job_queue([&tmp_path](std::string&) {
                std::ofstream ofs(tmp_path);
                ofs << "Hello, Acquire!";
                ofs.close();
                return true;
            });
            pool.await_stop();

            CHECK(fs::exists(tmp_path));

            retval = 1;
        } catch (const std::exception& exc) {
            LOGE("Caught exception: %s", exc.what());
        } catch (...) {
            LOGE("Caught unknown exception");
        }

        try {
            // cleanup
            if (fs::exists(tmp_path)) {
                fs::remove(tmp_path);
            }
        } catch (const std::exception& exc) {
            LOGE("Caught exception: %s", exc.what());
            retval = 0;
        }

        return retval;
    }
}

#endif // NO_UNIT_TESTS