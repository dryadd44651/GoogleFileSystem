cmake_minimum_required(VERSION 3.10)
project(distfs)

set(CMAKE_CXX_STANDARD 11)

find_package(Poco REQUIRED Foundation Util Net)

add_executable(difscs chunk_server.cpp chunk_server.h chunk_server_main.cpp common.cpp common.h)

target_link_libraries(difscs
    Poco::Foundation
    Poco::Util
    Poco::Net
)

add_executable(difsms meta_server.cpp meta_server.h meta_server_main.cpp common.cpp common.h)
target_link_libraries(difsms
    Poco::Foundation
    Poco::Util
    Poco::Net
)

add_executable(difsas access_server.cpp access_server.h access_server_main.cpp common.cpp common.h)

target_link_libraries(difsas
    Poco::Foundation
    Poco::Util
    Poco::Net
)