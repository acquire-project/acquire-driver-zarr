include(FetchContent)

FetchContent_Declare(
  minio
  GIT_REPOSITORY https://github.com/minio/minio-cpp.git
  GIT_TAG origin/main # TODO: replace with a specific commit or tag before merging
  GIT_SHALLOW true
)

FetchContent_MakeAvailable(minio)
