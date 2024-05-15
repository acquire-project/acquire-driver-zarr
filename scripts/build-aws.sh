# This script builds the AWS SDK for C++ and installs it into a specified directory.

# Parameters
# $BUILD_TYPE: The CMake build type (Release, RelWithDebInfo, Debug)
# $INSTALL_DIR: The directory where the AWS SDK for C++ will be installed (aws-sdk-cpp)
BUILD_TYPE=$1
INSTALL_DIR=$2

SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )" # Get the directory of this script
BASE_PATH="$(dirname $SCRIPT_PATH)" # Get the parent directory of the script directory
AWS_PATH="${BASE_PATH}/aws-sdk-cpp" # Get the directory of the AWS SDK for C++
BUILD_PATH="/tmp/aws-sdk-cpp-build-${BUILD_TYPE}" # Get the directory where the AWS SDK for C++ will be built

# Build the SDK
mkdir -p "${BUILD_PATH}"
pushd "${BUILD_PATH}" || exit 1
cmake -B . -S "${AWS_PATH}" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" -DBUILD_ONLY="s3" -DENABLE_TESTING=OFF
cmake --build . --config "${BUILD_TYPE}"
cmake --install . --config "${BUILD_TYPE}"
popd || exit 1 # ${BUILD_PATH}

# Cleanup
rm -rf "${BUILD_PATH}"
