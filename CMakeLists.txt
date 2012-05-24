cmake_minimum_required (VERSION 2.6)
project (smack)
# The version number.
set (smack_VERSION "0.1.0")

find_package(ZLIB REQUIRED)
find_package(Boost REQUIRED filesystem system thread)
set(CMAKE_CXX_FLAGS "-g -W -Wall")

include_directories("${PROJECT_BINARY_DIR}", "${PROJECT_SOURCE_DIR}/include")
FILE(GLOB headers "${CMAKE_CURRENT_SOURCE_DIR}/include/smack/*.hpp" "${CMAKE_CURRENT_SOURCE_DIR}/include/smack/*.h")
install(FILES ${headers} DESTINATION include/smack)

add_subdirectory(src)
add_subdirectory(example)

set(ARCHIVE_NAME ${CMAKE_PROJECT_NAME})
add_custom_target(dist
    COMMAND git archive --prefix=${ARCHIVE_NAME}-${smack_VERSION}/ HEAD
    | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}-${smack_VERSION}.tar.bz2
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
