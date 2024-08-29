#ifndef H_ACQUIRE_ZARR_ERRORS_V0
#define H_ACQUIRE_ZARR_ERRORS_V0

#ifdef __cplusplus
extern "C"
{
#endif

    typedef enum
    {
        ZarrError_Success = 0,
        ZarrError_InvalidArgument,
        ZarrError_Overflow,
        ZarrError_InvalidIndex,
        ZarrError_NotYetImplemented,
        ZarrError_Failure,
        ZarrError_InternalError,
        ZarrErrorCount,
    } ZarrError;

    const char* Zarr_get_error_message(ZarrError error);

#ifdef __cplusplus
}
#endif

#endif // H_ACQUIRE_ZARR_ERRORS_V0
