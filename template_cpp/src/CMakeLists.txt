# DO NOT CHANGE THE SYMBOLIC VARIABLE `SOURCES`.
# You can, however, change the list of files that comprise this variable.

include_directories(include)
set(SOURCES src/main.cpp src/hello.c src/perfect_links.cpp src/host_utils.cpp src/logger.cpp)

# DO NOT EDIT THE FOLLOWING LINES
find_package(Threads)
add_executable(da_proc ${SOURCES})
target_link_libraries(da_proc ${CMAKE_THREAD_LIBS_INIT})
