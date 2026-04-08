cmake_minimum_required(VERSION 3.28 FATAL_ERROR)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
set(CMAKE_VERBOSE_MAKEFILE OFF)
set(BUILD_SHARED_LIBS ON CACHE BOOL "Build shared libraries")

if(NOT SKBUILD_PROJECT_NAME)
  set(SKBUILD_PROJECT_NAME "scaler")
endif()

# Path to the version file
set(VERSION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/src/scaler/version.txt")

# Check if the version file exists
if(NOT EXISTS "${VERSION_FILE}")
    message(FATAL_ERROR "Version file not found: ${VERSION_FILE}")
endif()

# Read version from file
file(READ "${VERSION_FILE}" VERSION_CONTENTS)

# Strip whitespace from the version string
string(STRIP "${VERSION_CONTENTS}" PROJECT_VERSION)

project(
  ${SKBUILD_PROJECT_NAME}
  LANGUAGES C CXX
  VERSION ${PROJECT_VERSION}
)

configure_file(
	${PROJECT_SOURCE_DIR}/version.h.in
	${PROJECT_BINARY_DIR}/version.h
)

enable_testing()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
  message(STATUS "${PROJECT_NAME} defaulting to Release build")
elseif(NOT CMAKE_BUILD_TYPE STREQUAL "Debug" AND NOT CMAKE_BUILD_TYPE STREQUAL "Release")
  message(FATAL_ERROR "${PROJECT_NAME} invalid CMAKE_BUILD_TYPE: '${CMAKE_BUILD_TYPE}'. Must be 'Debug' or 'Release'.")
else()
  message(STATUS "${PROJECT_NAME} ${CMAKE_BUILD_TYPE} build")
endif()

# Compiler configuration
if(WIN32)
    # Windows
    message(STATUS "Windows")

    set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS on)
    add_compile_definitions(WIN32_LEAN_AND_MEAN NOMINMAX)
    add_compile_options("/EHsc")

elseif(APPLE)
    if(CMAKE_OSX_ARCHITECTURES MATCHES "arm64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
        # Mac Arm
        message(STATUS "macOS on ARM64")
    else()
        # Mac Intel
        message(STATUS "macOS on x86_64")
    endif()

    add_compile_options(-Wall -Wextra)
    add_link_options(-Wl,-rpath,@loader_path)

elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        # Linux GCC
        message(STATUS "Linux GCC ${CMAKE_CXX_COMPILER_VERSION}")

        add_compile_options(-Wall -Wsubobject-linkage)
        add_link_options(-Wl,-rpath,$ORIGIN)

    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang") # matches Clang and AppleClang
        #Linux Clang
        message(STATUS "Linux Clang ${CMAKE_CXX_COMPILER_VERSION}")

        add_compile_options(-Wall -stdlib=libc++)
        add_link_options(-stdlib=libc++)

        # needed for jthread in libc++
        add_compile_options(-fexperimental-library)
        add_link_options(-fexperimental-library)

    else()
        message(WARNING "Unknown compiler on Linux")
    endif()
else()
    message(WARNING "Unknown platform: ${CMAKE_SYSTEM_NAME}")
endif()

# Include Python-related utilities
include(cmake/ScalerPythonModule.cmake)

if(DEFINED ENV{CMAKE_PREFIX_PATH} AND NOT "$ENV{CMAKE_PREFIX_PATH}" STREQUAL "")
    file(TO_CMAKE_PATH "$ENV{CMAKE_PREFIX_PATH}" SCALER_ENV_CMAKE_PREFIX_PATH)
    list(PREPEND CMAKE_PREFIX_PATH ${SCALER_ENV_CMAKE_PREFIX_PATH})
endif()

if(DEFINED ENV{CapnProto_DIR} AND NOT "$ENV{CapnProto_DIR}" STREQUAL "" AND NOT DEFINED CapnProto_DIR)
    file(TO_CMAKE_PATH "$ENV{CapnProto_DIR}" CapnProto_DIR)
endif()

if(DEFINED CMAKE_PREFIX_PATH AND NOT "${CMAKE_PREFIX_PATH}" STREQUAL "")
    set(SCALER_NORMALIZED_CMAKE_PREFIX_PATH)
    foreach(SCALER_PREFIX_PATH IN LISTS CMAKE_PREFIX_PATH)
        file(TO_CMAKE_PATH "${SCALER_PREFIX_PATH}" SCALER_NORMALIZED_PREFIX_PATH)
        list(APPEND SCALER_NORMALIZED_CMAKE_PREFIX_PATH "${SCALER_NORMALIZED_PREFIX_PATH}")
    endforeach()
    set(CMAKE_PREFIX_PATH ${SCALER_NORMALIZED_CMAKE_PREFIX_PATH})
endif()

if(DEFINED CapnProto_DIR AND NOT "${CapnProto_DIR}" STREQUAL "")
    file(TO_CMAKE_PATH "${CapnProto_DIR}" CapnProto_DIR)
    set(SCALER_CAPNP_CONFIG_PATH "${CapnProto_DIR}/CapnProtoConfig.cmake")
endif()

message(STATUS "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}")
message(STATUS "CapnProto_DIR: ${CapnProto_DIR}")

if(DEFINED SCALER_CAPNP_CONFIG_PATH)
    message(STATUS "CapnProtoConfig path: ${SCALER_CAPNP_CONFIG_PATH}")
    if(EXISTS "${SCALER_CAPNP_CONFIG_PATH}")
        message(STATUS "CapnProtoConfig exists: TRUE")
    else()
        message(STATUS "CapnProtoConfig exists: FALSE")
    endif()
endif()

if(DEFINED SCALER_CAPNP_CONFIG_PATH AND EXISTS "${SCALER_CAPNP_CONFIG_PATH}")
    find_package(CapnProto CONFIG REQUIRED PATHS "${CapnProto_DIR}" NO_DEFAULT_PATH)
else()
    find_package(CapnProto CONFIG REQUIRED)
endif()
get_target_property(CAPNP_INCLUDE_DIRS CapnProto::capnp INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "Found Capnp includes in ${CAPNP_INCLUDE_DIRS}")

# Make LSP happy
include_directories(${CAPNP_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/src/cpp)

add_subdirectory(src/cpp/scaler)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_subdirectory(tests)
  add_subdirectory(examples)
endif()
