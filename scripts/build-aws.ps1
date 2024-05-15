# This script builds the AWS SDK for C++ and installs it into a specified directory.

# Parameters:
# BuildType: The CMake build type (Release, RelWithDebInfo, Debug)
# InstallDir (aws-sdk-cpp)
param(
    [string]$BuildType = "Release",
    [string]$InstallDir = "aws-sdk-cpp-install"
)

# Clone aws-sdk-cpp into a temporary directory
Push-Location $ENV:Temp
git clone --recursive https://github.com/aws/aws-sdk-cpp.git --branch 1.11.328

# Initialize submodules
Push-Location aws-sdk-cpp
git submodule update --init --recursive
Pop-Location # aws-sdk-cpp

# Build the SDK
mkdir aws-sdk-cpp-build-$BuildType

Push-Location aws-sdk-cpp-build-$BuildType
cmake -B . -S ../aws-sdk-cpp -DCMAKE_BUILD_TYPE="$BuildType" -DCMAKE_INSTALL_PREFIX="$InstallDir" -DBUILD_ONLY="s3" -DENABLE_TESTING=OFF
cmake --build . --config $BuildType
cmake --install . --config $BuildType
Pop-Location # aws-sdk-cpp-build

# Cleanup
Remove-Item -Recurse -Force aws-sdk-cpp
Remove-Item -Recurse -Force aws-sdk-cpp-build

Pop-Location # $ENV:Temp