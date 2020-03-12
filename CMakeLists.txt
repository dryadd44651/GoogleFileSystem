cmake_minimum_required(VERSION 3.10)
project(DistFSAll)

include(ExternalProject)
# Build poco as a static library and disable all the modules we don't need.
ExternalProject_Add(poco
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/external/poco-1.9.3
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/poco
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/poco
    CMAKE_ARGS
        "-DENABLE_MONGODB=OFF"
        "-DENABLE_REDIS=OFF"
        "-DENABLE_DATA_ODBC=OFF"
        "-DENABLE_DATA_MYSQL=OFF"
        "-DENABLE_PAGECOMPILER=OFF"
        "-DENABLE_PAGECOMPILER_FILE2PAGE=OFF"
        "-DPOCO_STATIC=ON"
    INSTALL_COMMAND ""
)

# Build our project
ExternalProject_Add(DistFS
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src"
    BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/DistFS"
    CMAKE_ARGS
        "-DPoco_DIR:PATH=${CMAKE_CURRENT_BINARY_DIR}/poco/Poco"
    INSTALL_COMMAND ""
    BUILD_ALWAYS ON
)

add_dependencies(DistFS poco)

add_custom_target(DistFSAll)
add_dependencies(DistFSAll DistFS)
