add_subdirectory(3rdParty)
add_subdirectory(acquire-device-kit)

set(tgt acquire-driver-zarr)
add_library(${tgt} MODULE
        prelude.h
        zarr.hh
        zarr.cpp
        zarr.raw.hh
        zarr.raw.cpp
        zarr.codec.hh
        zarr.blosc.hh
        zarr.blosc.cpp
        zarr.driver.c
)
target_enable_simd(${tgt})
target_link_libraries(${tgt} PRIVATE
        acquire-core-logger
        acquire-core-platform
        acquire-device-kit
        acquire-device-properties
        cblosc
        json
)
set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS ${tgt} LIBRARY DESTINATION lib)

function(target_add_zarr_video_sinks tgt)
        set(desc "Acquire Zarr storage")
        set(driver acquire-zarr-storage)

        set(extra_macro_args ${ARGN})
        list(LENGTH extra_macro_args num_extra_args)

        add_custom_command(
                TARGET ${tgt}
                POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy
                $<TARGET_FILE:${driver}>
                $<TARGET_FILE_DIR:${tgt}>
                COMMENT "COPY - ${desc} to build directory for ${tgt}"
        )
        add_dependencies(${tgt} ${driver})

        if(${num_extra_args} GREATER 0)
                list(GET extra_macro_args 0 install_destination)
                message(STATUS "Will install ${desc} to ${install_destination}")
                install(TARGETS ${driver} DESTINATION ${install_destination})
        endif()
endfunction()