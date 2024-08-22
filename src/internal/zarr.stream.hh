#pragma once

#include <cstddef> // size_t

#include "stream.settings.hh"

struct ZarrStream_s
{
    struct ZarrStreamSettings_s* settings;
    size_t version;
};
