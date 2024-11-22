cmake_minimum_required (VERSION 3.8)

project(join)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wextra-semi -g -fsanitize=address -fno-omit-frame-pointer")

include(GNUInstallDirs)

set(CMAKE_CXX_STANDARD 20)

include_directories(src)

add_executable(join src/main.cpp src/join.cpp)
