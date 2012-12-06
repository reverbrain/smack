add_executable(smack_test test.cpp)
target_link_libraries(smack_test smack)

add_executable(smack_reader read.cpp)
target_link_libraries(smack_reader smack)

install(TARGETS smack_test smack_reader
    RUNTIME DESTINATION bin
    )
