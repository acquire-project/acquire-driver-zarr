#ifndef H_ACQUIRE_STORAGE_ZARR_THREAD_POOL_V0
#define H_ACQUIRE_STORAGE_ZARR_THREAD_POOL_V0

#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>

namespace acquire::sink::zarr::common {
struct ThreadPool final
{
  public:
    using JobT = std::function<bool(std::string&)>;

    // The error handler `err` is called when a job returns false. This
    // can happen when the job encounters an error, or otherwise fails. The
    // std::string& argument to the error handler is a diagnostic message from
    // the failing job and is logged to the error stream by the Zarr driver when
    // the next call to `append()` is made.
    ThreadPool(unsigned int n_threads, std::function<void(const std::string&)> err);
    ~ThreadPool() noexcept;

    void push_to_job_queue(JobT&& job);

    /**
     * @brief Block until all jobs on the queue have processed, then spin down
     * the threads.
     * @note After calling this function, the job queue no longer accepts jobs.
     */
    void await_stop() noexcept;

  private:
    std::function<void(const std::string&)> error_handler_;

    std::vector<std::thread> threads_;
    mutable std::mutex jobs_mutex_;
    std::condition_variable cv_;
    std::queue<JobT> jobs_;

    std::atomic<bool> is_accepting_jobs_;

    std::optional<ThreadPool::JobT> pop_from_job_queue_() noexcept;
    [[nodiscard]] bool should_stop_() const noexcept;
    void thread_worker_();
};
}
#endif // H_ACQUIRE_STORAGE_ZARR_THREAD_POOL_V0
