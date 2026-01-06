cmake_minimum_required(VERSION 3.0)
project(rdma-anns)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")

set(CMAKE_BUILD_TYPE "Release")

message(STATUS "Build CoroMem library")

include(GNUInstallDirs)
include(CheckCXXCompilerFlag)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_COMPILER /data/share/users/xyzhi/spack/opt/spack/linux-debian9-skylake_avx512/gcc-8.2.0/gcc-11.4.0-dwr4wdspczmg4ssoib2ftvjodmdydhk2/bin/g++)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

SET(CMAKE_CXX_FLAGS  "-Ofast -lrt -DHAVE_CXX0X -march=native -fpic -w -fopenmp -ftree-vectorize -ftree-vectorizer-verbose=0 -shared-libgcc -fcoroutines -fopenmp" )

set(CMAKE_THREAD_LIBS_INIT "-lpthread")

find_package(Threads REQUIRED)
find_package(Boost 1.58.0 REQUIRED COMPONENTS serialization iostreams)
find_library(MEMCACHED_LIB memcached REQUIRED)

include_directories(./third_party)
set(THIRD_PARTY_DIR ${CMAKE_SOURCE_DIR}/third_party)
add_subdirectory(${THIRD_PARTY_DIR})

add_custom_target(lib)
add_subdirectory(coromem)

message(STATUS "Build RDMA library")

# for rdma deps
link_libraries(pthread /usr/lib/x86_64-linux-gnu/libibverbs.so.1 /usr/lib/x86_64-linux-gnu/librdmacm.so.1 ${MEMCACHED_LIB})
include_directories("${PROJECT_SOURCE_DIR}" "/data/share/project/grasper/rdma-core/build/include")

add_library(rdma_anns)
file(GLOB sources 
  ${CMAKE_SOURCE_DIR}/src/*.cc 
  ${CMAKE_SOURCE_DIR}/src/rdma/*.cc 
  ${CMAKE_SOURCE_DIR}/src/anns/*.cc 
)

target_sources(rdma_anns PRIVATE ${sources})

target_include_directories(rdma_anns PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)


set(POSSIBLE_OMP_PATHS "/data/share/users/xyzhi/new-spack/spack-0.22.2/opt/spack/linux-debian9-skylake_avx512/gcc-8.2.0/intel-oneapi-mkl-2024.0.0-a4e6uj4nsyq4llk43fbax6eobust7uug/compiler/latest/lib/libiomp5.so")
    
foreach(POSSIBLE_OMP_PATH ${POSSIBLE_OMP_PATHS})
    if (EXISTS ${POSSIBLE_OMP_PATH})
        get_filename_component(OMP_PATH ${POSSIBLE_OMP_PATH} DIRECTORY)
    endif()
endforeach()
link_directories(${OMP_PATH})

set(POSSIBLE_MKL_LIB_PATHS "/data/share/users/xyzhi/new-spack/spack-0.22.2/opt/spack/linux-debian9-skylake_avx512/gcc-8.2.0/intel-oneapi-mkl-2024.0.0-a4e6uj4nsyq4llk43fbax6eobust7uug/mkl/latest/lib/intel64/libmkl_core.so")

foreach(POSSIBLE_MKL_LIB_PATH ${POSSIBLE_MKL_LIB_PATHS})
    if (EXISTS ${POSSIBLE_MKL_LIB_PATH})
        get_filename_component(MKL_PATH ${POSSIBLE_MKL_LIB_PATH} DIRECTORY)
    endif()
endforeach()
set(POSSIBLE_MKL_INCLUDE_PATHS "/data/share/users/xyzhi/new-spack/spack-0.22.2/opt/spack/linux-debian9-skylake_avx512/gcc-8.2.0/intel-oneapi-mkl-2024.0.0-a4e6uj4nsyq4llk43fbax6eobust7uug/mkl/latest/include")
foreach(POSSIBLE_MKL_INCLUDE_PATH ${POSSIBLE_MKL_INCLUDE_PATHS})
    if (EXISTS ${POSSIBLE_MKL_INCLUDE_PATH})
        set(MKL_INCLUDE_PATH ${POSSIBLE_MKL_INCLUDE_PATH})
    endif()
endforeach()
MESSAGE(STATUS "index MKL_PATH: ${MKL_PATH}")
link_directories(${MKL_PATH})
include_directories(${MKL_INCLUDE_PATH})

find_package(Boost COMPONENTS program_options system filesystem)

link_libraries(mkl_intel_ilp64 mkl_intel_thread mkl_core iomp5 pthread m dl ${Boost_LIBRARIES})

add_library(rdma_index)
file(GLOB index_sources 
  ${CMAKE_SOURCE_DIR}/src/index/*.cc
)
target_sources(rdma_index PRIVATE ${index_sources})
target_include_directories(rdma_index PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

add_subdirectory(tests)


