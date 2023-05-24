#ifndef H_ACQUIRE_STORAGE_ZARR_ENCODER_V0
#define H_ACQUIRE_STORAGE_ZARR_ENCODER_V0

#ifdef __cplusplus

#include <string>
#include <vector>

#include "platform.h"

#include "prelude.h"
#include "json.hpp"

namespace acquire::sink::zarr {

struct BloscCompressor
{
    static constexpr char id_[] = "blosc";
    std::string codec_id_;
    int clevel_;
    int shuffle_;

    BloscCompressor();
    BloscCompressor(const std::string& codec_id, int clevel, int shuffle);
};

void
to_json(nlohmann::json&, const BloscCompressor&);

void
from_json(const nlohmann::json&, BloscCompressor&);

template<typename E>
concept Encoder = requires(E encoder,
                           const uint8_t* beg,
                           const uint8_t* end,
                           size_t bpp,
                           size_t buf_size,
                           const std::string& file_path) {
    // clang-format off
    { encoder.write(beg, end) } -> std::convertible_to<size_t>;
    { encoder.flush() } -> std::convertible_to<size_t>;
    encoder.set_bytes_per_pixel(bpp);
    encoder.allocate_buffer(buf_size);
    encoder.set_file_path(file_path);
    encoder.close_file();
    { encoder.get_compressor() } -> std::convertible_to<BloscCompressor*>;
    // clang-format on
};

struct BaseEncoder
{
  public:
    virtual ~BaseEncoder() noexcept;

    size_t write(const uint8_t* beg, const uint8_t* end);
    size_t flush();
    void set_bytes_per_pixel(size_t bpp);
    void allocate_buffer(size_t buf_size);

    void set_file_path(const std::string& file_path);
    void close_file();

    virtual BloscCompressor* get_compressor() = 0;

  protected:
    std::vector<uint8_t> buf_;
    size_t cursor_;
    size_t bytes_per_pixel_;
    std::string path_;
    struct file* file_handle_;
    bool file_has_been_created_;

    BaseEncoder();
    void open_file();

    virtual size_t flush_impl() = 0;
    virtual void open_file_impl() = 0;
};
} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // H_ACQUIRE_STORAGE_ZARR_ENCODER_V0
