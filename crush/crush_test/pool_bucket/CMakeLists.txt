cmake_minimum_required(VERSION 2.8.9)
project(pool_bucket)

#Bring the headers, such as pool_buckett.h into the project
include_directories(include)
set(CMAKE_CXX_STANDARD 11)
 
#you can manually add the sources using the set command:
#set(SOURCES src/mainapp.cpp src/Student.cpp)
 
#However, the file(GLOB...) allows for wildcard additions:
file(GLOB SOURCES "*.cc")
 
add_executable(pool_bucket ${SOURCES})
