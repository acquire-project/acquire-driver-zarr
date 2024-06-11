# This script builds the AWS SDK for C++ and installs it into a specified directory.

# Parameters:
# BuildType: The CMake build type (Release, RelWithDebInfo, Debug)
# InstallDir (aws-sdk-cpp)
param(
    [string]$BuildType = "Release",
    [string]$InstallDir = "aws-sdk-cpp-install"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$baseDir = Split-Path -Parent $scriptDir
$awsDir = Join-Path $baseDir "aws-sdk-cpp"
$buildDir = Join-Path $ENV:Temp "aws-sdk-cpp-build-$BuildType"

# Build the SDK
if (!(Test-Path $buildDir))
{
    New-Item -ItemType Directory -Path $buildDir
}
Push-Location $buildDir

cmake -B . -S $awsDir -DCMAKE_BUILD_TYPE="$BuildType" -DCMAKE_INSTALL_PREFIX="$InstallDir" -DBUILD_ONLY="s3" -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=OFF -DFORCE_SHARED_CRT=ON -DCMAKE_CXX_FLAGS="/EHsc /MT"
cmake --build . --config $BuildType
cmake --install . --config $BuildType
Pop-Location # $buildDir

# Cleanup
if (Test-Path $buildDir)
{
    Remove-Item -Recurse -Force $buildDir
}
