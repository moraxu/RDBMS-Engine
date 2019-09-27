cmake_minimum_required(VERSION 3.14)
project(cs222_fall19)

set(CMAKE_CXX_STANDARD 11)

add_custom_target(clean-all
        COMMAND rm Index* Indices* left* right* large* group* *out Tables Columns tbl_*)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -O1 -g  -fno-omit-frame-pointer")
if (CMAKE_BUILD_TYPE MATCHES Debug)
    add_definitions(-DDEBUG=1)

endif()

add_library(PFM ./rbf/pfm.cc)
add_library(RBFM ./rbf/rbfm.cc)


file(GLOB files rbf/rbftest*.cc)
foreach(file ${files})
    get_filename_component(name ${file} NAME_WE)
    add_executable(${name} ${file})
    target_link_libraries(${name} RBFM PFM)
endforeach()