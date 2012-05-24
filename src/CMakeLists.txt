add_library(smack SHARED key.cpp file.cpp mmap.cpp bloom.cpp logger.cpp crypto/sha512.c smack.cpp)
target_link_libraries(smack ${Boost_FILESYSTEM_LIBRARY}
	${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${ZLIB_LIBRARY})
set_target_properties(smack PROPERTIES VERSION ${smack_VERSION} SOVERSION ${smack_VERSION})

SET(LIB_SUFFIX "")
IF(CMAKE_SIZEOF_VOID_P EQUAL 8)
    SET(LIB_SUFFIX 64)
ENDIF(CMAKE_SIZEOF_VOID_P EQUAL 8)

SET(CMAKE_INSTALL_LIBDIR lib${LIB_SUFFIX} CACHE PATH "Output directory for libraries")

install(TARGETS smack LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
