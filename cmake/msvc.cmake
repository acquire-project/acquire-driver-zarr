option(ACQUIRE_MSVC_USE_MD "When ON, prefer linking to the Multithreaded DLL Microsoft C Runtime library (use /MD)")

if(ACQUIRE_MSVC_USE_MD)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
else()
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()