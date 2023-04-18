include(cmake/TargetArch.cmake)

function(target_enable_simd tgt)
    target_architecture(arch)
    set(is_gcc_like "$<OR:$<COMPILE_LANG_AND_ID:CXX,AppleClang,Clang,GNU>,$<COMPILE_LANG_AND_ID:C,AppleClang,Clang,GNU>>")
    set(is_msvc_like "$<OR:$<COMPILE_LANG_AND_ID:CXX,MSVC>,$<COMPILE_LANG_AND_ID:C,MSVC>>")
    set(is_arch_x64 "$<STREQUAL:${arch},x86_64>")
    target_compile_options(${tgt} PRIVATE
            $<$<AND:${is_arch_x64},${is_gcc_like}>:-mavx2>
            $<$<AND:${is_arch_x64},${is_msvc_like}>:/arch:AVX2>
            )
endfunction()
