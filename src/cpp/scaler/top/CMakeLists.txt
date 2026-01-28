# Try to find pre-installed FTXUI, otherwise fetch it
find_package(ftxui 5.0.0 QUIET)
if(ftxui_FOUND)
    message(STATUS "Found pre-installed FTXUI: ${ftxui_DIR}")
else()
    message(STATUS "FTXUI not found, fetching from GitHub...")
    include(FetchContent)
    set(BUILD_SHARED_LIBS_BACKUP ${BUILD_SHARED_LIBS})
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
        ftxui
        GIT_REPOSITORY https://github.com/ArthurSonzogni/ftxui
        GIT_TAG v5.0.0
    )
    set(FTXUI_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(ftxui)
    set(BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS_BACKUP} CACHE BOOL "" FORCE)
endif()

# Generate Cap'n Proto C++ for status and message schemas
set(CAPNPC_SRC_PREFIX "${PROJECT_SOURCE_DIR}/src/scaler/protocol/capnp" CACHE STRING "" FORCE)
set(CAPNPC_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/capnp_gen" CACHE STRING "" FORCE)
file(MAKE_DIRECTORY "${CAPNPC_OUTPUT_DIR}")

capnp_generate_cpp(COMMON_CAPNP_SRCS COMMON_CAPNP_HEADERS
    "${PROJECT_SOURCE_DIR}/src/scaler/protocol/capnp/common.capnp")
capnp_generate_cpp(STATUS_CAPNP_SRCS STATUS_CAPNP_HEADERS
    "${PROJECT_SOURCE_DIR}/src/scaler/protocol/capnp/status.capnp")
capnp_generate_cpp(MESSAGE_CAPNP_SRCS MESSAGE_CAPNP_HEADERS
    "${PROJECT_SOURCE_DIR}/src/scaler/protocol/capnp/message.capnp")

add_library(top_capnp_objs OBJECT
    ${COMMON_CAPNP_SRCS}
    ${COMMON_CAPNP_HEADERS}
    ${STATUS_CAPNP_SRCS}
    ${STATUS_CAPNP_HEADERS}
    ${MESSAGE_CAPNP_SRCS}
    ${MESSAGE_CAPNP_HEADERS}
)

target_include_directories(top_capnp_objs PUBLIC ${CAPNPC_OUTPUT_DIR})

# Main executable
add_executable(scaler_top
    main.cpp
    scaler_top.cpp
    state/scheduler_state.cpp
    ui/top_screen.cpp
    ui/formatters.cpp
    protocol/message_deserializer.cpp
    network/ymq_subscriber.cpp
)

target_include_directories(scaler_top PRIVATE
    ${PROJECT_SOURCE_DIR}/src/cpp
    ${CAPNPC_OUTPUT_DIR}
)

target_link_libraries(scaler_top PRIVATE
    top_capnp_objs
    ymq_objs
    CapnProto::capnp
    CapnProto::kj
    ftxui::screen
    ftxui::dom
    ftxui::component
)

install(TARGETS scaler_top
    RUNTIME DESTINATION bin
)
