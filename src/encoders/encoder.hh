#ifndef H_ACQUIRE_STORAGE_ENCODER_V0
#define H_ACQUIRE_STORAGE_ENCODER_V0

#ifndef __cplusplus
#error "This header requires C++20"
#endif

#include "../prelude.h"

#include "device/props/components.h"

template<typename EncoderT>
concept Encoder = requires(EncoderT e,
                           uint8_t* bytes_out,
                           size_t nbytes_out,
                           const uint8_t* bytes_in,
                           size_t nbytes_in) {
    {
        e.encode(bytes_out, nbytes_out, bytes_in, nbytes_in)
    } -> std::unsigned_integral; // number of bytes written
};

#endif // H_ACQUIRE_STORAGE_ENCODER_V0
