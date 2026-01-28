#!/bin/bash -e
#
# This script builds and installs in-place the C++ components.
#
# If `--clean` is specified, remove any existing cached build files before building.
#
# Usage:
#      ./scripts/build.sh [--clean]

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"   # e.g. linux or darwin
ARCH="$(uname -m)"                              # e.g. x86_64 or arm64

BUILD_DIR="build_${OS}_${ARCH}"
BUILD_PRESET="${OS}-${ARCH}"

# Third-party libraries prefix (for libuv, etc.)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
THIRDPARTY_PREFIX="${PROJECT_DIR}/thirdparties/installed"

if [[ "$1" == "--clean" ]]; then
    rm -rf $BUILD_DIR
    rm -f src/scaler/protocol/capnp/*.c++
    rm -f src/scaler/protocol/capnp/*.h
fi

echo "Build directory: $BUILD_DIR"
echo "Build preset: $BUILD_PRESET"

# Configure with third-party prefix path
CMAKE_ARGS=()
if [[ -d "$THIRDPARTY_PREFIX" ]]; then
    echo "Using third-party libraries from: $THIRDPARTY_PREFIX"
    CMAKE_ARGS+=("-DCMAKE_PREFIX_PATH=$THIRDPARTY_PREFIX")
fi

cmake --preset $BUILD_PRESET "${CMAKE_ARGS[@]}"

# Build
cmake --build --preset $BUILD_PRESET

# Install
cmake --install $BUILD_DIR
