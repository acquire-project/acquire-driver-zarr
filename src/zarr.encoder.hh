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
    int available_threads_;

    BloscCompressor();
    BloscCompressor(const std::string& codec_id, int clevel, int shuffle);

    static std::vector<std::string> supported_codecs();
};

void
to_json(nlohmann::json&, const BloscCompressor&);
void
from_json(const nlohmann::json&, BloscCompressor&);

struct Encoder
{
  public:
    Encoder() = delete;
    explicit Encoder(size_t buffer_size);
    virtual ~Encoder() noexcept;

    void set_bytes_per_pixel(size_t bpp);
    size_t write(const uint8_t* beg, const uint8_t* end);
    size_t flush();

    void set_file_path(const std::string& file_path);
    void close_file();

  protected:
    std::vector<uint8_t> buf_;
    size_t cursor_;
    size_t bytes_per_pixel_;
    std::string path_;
    struct file* file_handle_;
    bool file_has_been_created_;

    void open_file();

    virtual size_t flush_impl() = 0;
    virtual void open_file_impl() = 0;
};

struct RawEncoder final : public Encoder
{
  public:
    RawEncoder() = delete;
    explicit RawEncoder(size_t bytes_per_tile);
    ~RawEncoder() noexcept override;

  private:
    size_t file_offset_;

    size_t flush_impl() override;
    void open_file_impl() override;
};

struct BloscEncoder final : public Encoder
{
  public:
    BloscEncoder() = delete;
    BloscEncoder(const BloscCompressor& compressor, size_t bytes_per_chunk);
    ~BloscEncoder() noexcept override;

  private:
    ///< Compression parameters
    std::string codec_id_;
    int clevel_;
    int shuffle_;

    size_t flush_impl() override;
    void open_file_impl() override;
};
} // namespace acquire::sink::zarr

#endif // __cplusplus
#endif // H_ACQUIRE_STORAGE_ZARR_ENCODER_V0
