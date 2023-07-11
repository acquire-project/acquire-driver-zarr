// This is a "unit test" driver.
//
// Adding unit test functions here will run them as part of the CTest suite
// in a standardized fashion.
//
// Unit tests should be focused on testing the smallest logically isolated
// parts of the code. Practically, this means they should live close to the
// code they're testing. That is usually under the public interface
// defined by this module - if you're test uses a private interface that's a
// good sign it might be a unit test.
//
// Adding a new unit test:
// 1. Define your unit test in the same source file as what you're testing.
// 2. Add it to the declarations list below. See TEST DECLARATIONS.
// 3. Add it to the test list. See TEST LIST.
//
// Template:
//
// ```c
//      #ifndef NO_UNIT_TESTS
//      int
//      unit_test__my_descriptive_test_name()
//      {
//          // do stuff
//          return 1; // success
//      Error:
//          return 0; // failure
//      }
//      #endif // NO_UNIT_TESTS
// ```

#include "platform.h"
#include "logger.h"

#include <cstdio>
#include <stdexcept>
#include <vector>

#define L (aq_logger)
#define LOG(...) L(0, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define ERR(...) L(1, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)

void
reporter(int is_error,
         const char* file,
         int line,
         const char* function,
         const char* msg)
{
    fprintf(is_error ? stderr : stdout,
            "%s%s(%d) - %s: %s\n",
            is_error ? "ERROR " : "",
            file,
            line,
            function,
            msg);
}

typedef struct Driver* (*init_func_t)(void (*reporter)(int is_error,
                                                       const char* file,
                                                       int line,
                                                       const char* function,
                                                       const char* msg));
//
//      TEST DRIVER
//

int
main()
{
    logger_set_reporter(reporter);
    struct lib lib = { 0 };
    if (!lib_open_by_name(&lib, "acquire-driver-zarr")) {
        ERR("Failed to open \"acquire-driver-zarr\".");
        exit(2);
    }

    struct testcase
    {
        const char* name;
        int (*test)();
    };
    const std::vector<testcase> tests{
#define CASE(e) { .name = #e, .test = (int (*)())lib_load(&lib, #e) }
        CASE(unit_test__padding),
        CASE(unit_test__average_plane),
#undef CASE
    };

    bool any = false;
    for (const auto& test : tests) {
        LOG("Running %s", test.name);
        if (!(test.test())) {
            ERR("unit test failed: %s", test.name);
            any = true;
        }
    }
    lib_close(&lib);
    return any;
}
