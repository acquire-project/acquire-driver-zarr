set(pwd ${CMAKE_CURRENT_LIST_DIR})

message(STATUS "nlohmann json: ${pwd}")
set(tgt json)
add_library(${tgt} INTERFACE)
target_include_directories(${tgt} INTERFACE ${pwd}/inc)
