# This script builds and installs in-place the C++ components.
#
# If `--clean` is specified, remove any existing cached build files before building.
#
# Usage:
#      ./scripts/build.ps1 [--clean]

$ErrorActionPreference = "Stop"
$OS = "windows"
$ARCH = "x64"
$BUILD_DIR = "build_${OS}_${ARCH}"
$BUILD_PRESET = "${OS}-${ARCH}"

# Third-party libraries prefix (for libuv, etc.)
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECT_DIR = Split-Path -Parent $SCRIPT_DIR
$THIRDPARTY_PREFIX = Join-Path $PROJECT_DIR "thirdparties\installed"

if ($args[0] -eq "--clean") {
    # Clean up previous build artifacts
    if (Test-Path $BUILD_DIR) {
        Remove-Item -Recurse -Force $BUILD_DIR
    }
    Get-ChildItem "scaler/protocol/capnp" -Include *.c++, *.h -ErrorAction SilentlyContinue | Remove-Item -Force
}

Write-Host "Build directory: $BUILD_DIR"
Write-Host "Build preset: $BUILD_PRESET"

# Configure with third-party prefix path
$CMAKE_ARGS = @()
if (Test-Path $THIRDPARTY_PREFIX) {
    Write-Host "Using third-party libraries from: $THIRDPARTY_PREFIX"
    $CMAKE_ARGS += "-DCMAKE_PREFIX_PATH=$THIRDPARTY_PREFIX"
}

cmake --preset $BUILD_PRESET @CMAKE_ARGS

# Build
cmake --build --preset $BUILD_PRESET

# Install
cmake --install $BUILD_DIR
