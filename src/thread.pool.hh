#ifndef H_ACQUIRE_STORAGE_ZARR_THREAD_POOL_V0
#define H_ACQUIRE_STORAGE_ZARR_THREAD_POOL_V0

#ifdef __cplusplus

#include <thread>
#include <vector>

namespace acquire::sink::zarr {

class ThreadPool final
{
  public:
    ThreadPool();
    ~ThreadPool();

    [[nodiscard]] size_t capacity() const noexcept;
    [[nodiscard]] size_t nthreads() const noexcept;

    template<class F, class... Args>
    bool emplace(F&& f, Args&&... args);

    void join_all();

  private:
    std::vector<std::thread> threads_;
};

template<class F, class... Args>
bool
ThreadPool::emplace(F&& f, Args&&... args)
{
    if (threads_.size() < threads_.capacity()) {
        threads_.emplace_back(std::forward<F>(f), std::forward<Args>(args)...);
        return true;
    }
    return false;
}

} // namespace acquire::sink::zarr

#endif // __cplusplus

#endif // H_ACQUIRE_STORAGE_ZARR_THREAD_POOL_V0
