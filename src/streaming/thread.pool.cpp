#include "thread.pool.hh"

zarr::ThreadPool::ThreadPool(unsigned int n_threads,
                             std::function<void(const std::string&)> err)
  : error_handler_{ err }
  , is_accepting_jobs_{ true }
{
    n_threads = std::clamp(
      n_threads, 1u, std::max(std::thread::hardware_concurrency(), 1u));

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

bool
zarr::ThreadPool::push_to_job_queue(JobT&& job)
{
    std::unique_lock lock(jobs_mutex_);
    if (!is_accepting_jobs_) {
        return false;
    }

    jobs_.push(std::move(job));
    cv_.notify_one();

    return true;
}

void
zarr::ThreadPool::await_stop() noexcept
{
    {
        std::scoped_lock lock(jobs_mutex_);
        is_accepting_jobs_ = false;

        cv_.notify_all();
    }

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
}