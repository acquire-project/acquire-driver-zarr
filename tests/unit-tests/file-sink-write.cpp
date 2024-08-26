#include "file.sink.hh"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "Check failed: %s\n", #cond);                      \
            return 1;                                                          \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b) CHECK((a) == (b))

namespace fs = std::filesystem;

int
main()
{
    int retval = 0;
    fs::path tmp_path = fs::temp_directory_path() / TEST;

    CHECK(!fs::exists(tmp_path));
    {
        const uint8_t str[] = "Hello, Acquire!";
        zarr::FileSink sink(tmp_path.string());
        sink.write(0, str, sizeof(str) - 1);
    }

    // The file tmp_path should now contain the string "Hello, world!\n".
    CHECK(fs::exists(tmp_path));

    std::ifstream ifs(tmp_path);
    CHECK(ifs.is_open());

    std::string contents;
    while (!ifs.eof()) {
        std::getline(ifs, contents);
    }
    ifs.close();

    if (contents != "Hello, Acquire!") {
        fprintf(stderr,
                "Expected 'Hello, Acquire!' but got '%s'\n",
                contents.c_str());
        retval = 1;
    }
    goto Cleanup;

Finalize:
    return retval;

Cleanup:
    std::error_code ec;
    if (!fs::remove(tmp_path, ec)) {
        fprintf(stderr, "Failed to remove file: %s\n", ec.message().c_str());
        retval = 1;
    }

    goto Finalize;
}