add_executable(smack_test test.cpp)
target_link_libraries(smack_test smack)

add_executable(smack_index_iter iter.cpp)
target_link_libraries(smack_index_iter smack)
