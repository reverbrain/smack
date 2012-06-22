add_library(smack SHARED key.cpp file.cpp mmap.cpp bloom.cpp logger.cpp crypto/sha512.c smack.cpp)
target_link_libraries(smack ${Boost_FILESYSTEM_LIBRARY} ${Boost_IOSTREAMS_LIBRARY}
	${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${SNAPPY_LIBRARIES})
set_target_properties(smack PROPERTIES VERSION ${smack_VERSION} SOVERSION ${smack_VERSION})

IF (NOT CMAKE_INSTALL_LIBDIR)
SET(CMAKE_INSTALL_LIBDIR lib CACHE PATH "Output directory for libraries")
ENDIF (NOT CMAKE_INSTALL_LIBDIR)

install(TARGETS smack LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
