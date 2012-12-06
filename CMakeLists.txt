cmake_minimum_required (VERSION 2.6)
project (smack)

# The version number.
FILE (READ "${CMAKE_CURRENT_SOURCE_DIR}/debian/changelog" DEBCHANGELOG)
string(REGEX MATCH "([0-9]+\\.[0-9]+\\.[0-9]+)" DEBFULLVERSION "${DEBCHANGELOG}")
STRING (REGEX MATCH "([0-9]+\\.[0-9]+)" SMACK_VERSION_ABI "${DEBFULLVERSION}")
STRING (REGEX MATCH "([0-9]+$)" SMACK_VERSION_MINOR "${DEBFULLVERSION}")
set(${smack_VERSION} "${SMACK_VERSION_ABI}.${SMACK_VERSION_MINOR}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

OPTION (ENABLE_EXAMPLES "Build examples" OFF)

find_package(SNAPPY REQUIRED)
find_package(Boost REQUIRED filesystem system thread iostreams)
set(CMAKE_CXX_FLAGS "-g -W -Wall")

include_directories("${PROJECT_BINARY_DIR}" "${PROJECT_SOURCE_DIR}/include" ${SNAPPY_INCLUDE_DIR} ${Boost_INCLUDE_DIRS})
FILE(GLOB headers "${CMAKE_CURRENT_SOURCE_DIR}/include/smack/*.hpp" "${CMAKE_CURRENT_SOURCE_DIR}/include/smack/*.h")
install(FILES ${headers} DESTINATION include/smack)

add_subdirectory(src)

IF (ENABLE_EXAMPLES)
	add_subdirectory(example)
ENDIF (ENABLE_EXAMPLES)

set(ARCHIVE_NAME ${CMAKE_PROJECT_NAME})
add_custom_target(dist
    COMMAND git archive --prefix=${ARCHIVE_NAME}-${smack_VERSION}/ HEAD
    | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}-${smack_VERSION}.tar.bz2
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
