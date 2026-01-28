# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

# Parameters:
#
# BOOST_ROOT:
#   Specify root of the Boost library if Boost cannot be auto-detected. On Windows, a fallback to a
#   downloaded nuget version will be used if Boost cannot be found.
#
# DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS:
#   This is a work-in-progress feature, not completed yet. The core DiskANN library will be split into
#   build-related and search-related functionality. In build-related functionality, when using tcmalloc,
#   it's possible to release memory that's free but reserved by tcmalloc. Setting this to true enables
#   such behavior.
#   Contact for this feature: gopalrs.


# Some variables like MSVC are defined only after project(), so put that first.
# cmake_minimum_required(VERSION 3.15)
# project(diskann)

set(PROJECT_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/diskann")
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")


include_directories(${PROJECT_SOURCE_DIR}/include)

if(NOT PYBIND)
set(DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS ON)
endif()
# It's necessary to include tcmalloc headers only if calling into MallocExtension interface.
# For using tcmalloc in DiskANN tools, it's enough to just link with tcmalloc.
if (DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS)
include_directories(${PROJECT_SOURCE_DIR}/gperftools/src)
endif()

include_directories(/usr/local/include)
# FAISS 未使用，注释掉
# include_directories(./3rdparty/faiss)
# link_libraries(/data/share/users/xyzhi/new-spack/spack-0.22.2/opt/spack/linux-debian9-skylake_avx512/gcc-8.2.0/faiss-1.7.4-7lwxh7hvrens6htzefdgvibwohkv7pdh/lib/libfaiss.a)

#OpenMP
# find_package(OpenMP)

# if (OPENMP_FOUND)
#     set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
#     set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
# else()
# message(FATAL_ERROR "No OpenMP support")
# endif()
    
# DiskANN core uses header-only libraries. Only DiskANN tools need program_options which has a linker library,
# but its size is small. Reduce number of dependent DLLs by linking statically.
if(NOT MSVC)
find_package(Boost COMPONENTS program_options)
endif()

if (NOT Boost_FOUND)
    message(FATAL_ERROR "Couldn't find Boost dependency")
endif()

include_directories(${Boost_INCLUDE_DIR})
set(POSSIBLE_OMP_PATHS 
    "/usr/lib/x86_64-linux-gnu/libiomp5.so"
    "/usr/lib/llvm-18/lib/libiomp5.so"
)
    
foreach(POSSIBLE_OMP_PATH ${POSSIBLE_OMP_PATHS})
    if (EXISTS ${POSSIBLE_OMP_PATH})
        get_filename_component(OMP_PATH ${POSSIBLE_OMP_PATH} DIRECTORY)
    endif()
endforeach()

if(NOT OMP_PATH)
    message(FATAL_ERROR "Could not find Intel OMP in standard locations; use -DOMP_PATH to specify the install location for your environment")
endif()
link_directories(${OMP_PATH})

set(POSSIBLE_MKL_LIB_PATHS 
    "/usr/lib/x86_64-linux-gnu/libmkl_core.so"
)

foreach(POSSIBLE_MKL_LIB_PATH ${POSSIBLE_MKL_LIB_PATHS})
    if (EXISTS ${POSSIBLE_MKL_LIB_PATH})
        get_filename_component(MKL_PATH ${POSSIBLE_MKL_LIB_PATH} DIRECTORY)
    endif()
endforeach()

set(POSSIBLE_MKL_INCLUDE_PATHS 
    "/usr/include/mkl"
)

foreach(POSSIBLE_MKL_INCLUDE_PATH ${POSSIBLE_MKL_INCLUDE_PATHS})
    if (EXISTS ${POSSIBLE_MKL_INCLUDE_PATH})
        set(MKL_INCLUDE_PATH ${POSSIBLE_MKL_INCLUDE_PATH})
    endif()
endforeach()
if(NOT MKL_PATH)
    message(FATAL_ERROR "Could not find Intel MKL in standard locations; use -DMKL_PATH to specify the install location for your environment")
elseif(NOT MKL_INCLUDE_PATH)
    message(FATAL_ERROR "Could not find Intel MKL in standard locations; use -DMKL_INCLUDE_PATH to specify the install location for headers for your environment")
endif()
if (EXISTS ${MKL_PATH}/libmkl_def.so.2)
    set(MKL_DEF_SO ${MKL_PATH}/libmkl_def.so.2)
elseif(EXISTS ${MKL_PATH}/libmkl_def.so)
    set(MKL_DEF_SO ${MKL_PATH}/libmkl_def.so)
else()
    message(FATAL_ERROR "Despite finding MKL, libmkl_def.so was not found in expected locations.")
endif()
link_directories(${MKL_PATH})
include_directories(${MKL_INCLUDE_PATH})

# compile flags and link libraries
add_compile_options(-m64 -Wl,--no-as-needed)
if (NOT PYBIND)
    link_libraries(mkl_intel_ilp64 mkl_intel_thread mkl_core iomp5 pthread m dl)
else()
    # static linking for python so as to minimize customer dependency issues
    link_libraries(
            ${MKL_PATH}/libmkl_intel_ilp64.a
            ${MKL_PATH}/libmkl_intel_thread.a
            ${MKL_PATH}/libmkl_core.a
            ${MKL_DEF_SO}
            iomp5
            pthread
            m
            dl
    )
endif()

# find_package(MKL)

# if(MKL_FOUND)
#     message(STATUS "MKL found, MKL Include dirs: ${MKL_INCLUDE_DIRS}, MKL Libraries: ${MKL_LIBRARIES}")
#     include_directories(/usr/include/mkl)
#     link_libraries(${MKL_LIBRARIES})
# else()
#     message(FATAL_ERROR "No MKL support")
# endif()

add_definitions(-DMKL_ILP64)

# Section for tcmalloc. The DiskANN tools are always linked to tcmalloc. For Windows, they also need to
# force-include the _tcmalloc symbol for enabling tcmalloc.
#
# The DLL itself needs to be linked to tcmalloc only if DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS
# is enabled.
set(DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS "-ltcmalloc")

if (DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS)
    add_definitions(-DRELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS)
endif()

if (NOT MSVC)
    # 使用系统 libaio
    set(DISKANN_ASYNC_LIB aio)
endif()


set(ENV{TCMALLOC_LARGE_ALLOC_REPORT_THRESHOLD} 500000000000)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -mfma -msse2 -ftree-vectorize -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -fopenmp -fopenmp-simd -funroll-loops -Wfatal-errors -DUSE_AVX2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -fPIC")


file(GLOB CPP_SOURCES diskann/src/*.cpp)
if (RESTAPI)
    list(APPEND CPP_SOURCES diskann/src/restapi/search_wrapper.cpp diskann/src/restapi/server.cpp)
endif()
add_library(diskann ${CPP_SOURCES})

if (RESTAPI)
    add_subdirectory(diskann/apps/restapi)
endif()

include(diskann/clang-format.cmake)

if(PYBIND)
    add_subdirectory(diskann/python)
endif()


