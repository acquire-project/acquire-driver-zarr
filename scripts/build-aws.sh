# This script builds the AWS SDK for C++ and installs it into a specified directory.

# Parameters
# $BUILD_TYPE: The CMake build type (Release, RelWithDebInfo, Debug)
# $INSTALL_DIR: The directory where the AWS SDK for C++ will be installed (aws-sdk-cpp)
BUILD_TYPE=$1
INSTALL_DIR=$2

# Clone aws-sdk-cpp into a temporary directory
pushd /tmp || exit 1
git clone --recursive https://github.com/aws/aws-sdk-cpp.git --branch 1.11.328

# Initialize submodules
pushd aws-sdk-cpp || exit 1
git submodule update --init --recursive
popd || exit 1 # aws-sdk-cpp

# Build the SDK
mkdir -p "aws-sdk-cpp-build-${BUILD_TYPE}"

pushd "aws-sdk-cpp-build-${BUILD_TYPE}" || exit 1
cmake -B . -S ../aws-sdk-cpp -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" -DBUILD_ONLY="s3" -DENABLE_TESTING=OFF
cmake --build . --config "${BUILD_TYPE}"
cmake --install . --config "${BUILD_TYPE}"
popd || exit 1 # aws-sdk-cpp-build

# Cleanup
rm -rf aws-sdk-cpp
rm -rf aws-sdk-cpp-build

popd || exit 1 # /tmp