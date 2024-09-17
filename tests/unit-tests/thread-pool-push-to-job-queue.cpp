#include "thread.pool.hh"
#include "unit.test.macros.hh"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>

namespace fs = std::filesystem;

int
main()
{
    int retval = 0;

    fs::path tmp_path = fs::temp_directory_path() / TEST;
    CHECK(!fs::exists(tmp_path));

    zarr::ThreadPool pool{ 1, [](const std::string&) {} };

    CHECK(pool.push_to_job_queue([&tmp_path](std::string&) {
        std::ofstream ofs(tmp_path);
        ofs << "Hello, Acquire!";
        ofs.close();
        return true;
    }));
    pool.await_stop();

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