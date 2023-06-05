#include "thread.pool.hh"

namespace acquire::sink::zarr {
ThreadPool::ThreadPool()
{
    threads_.reserve(std::thread::hardware_concurrency());
}

ThreadPool::~ThreadPool()
{
    join_all();
}

size_t
ThreadPool::capacity() const noexcept
{
    return threads_.capacity();
}

size_t
ThreadPool::nthreads() const noexcept
{
    return threads_.size();
}

void
ThreadPool::join_all()
{
    for (auto& t : threads_) {
        if (t.joinable())
            t.join();
    }
}

} // namespace acquire::sink::zarr