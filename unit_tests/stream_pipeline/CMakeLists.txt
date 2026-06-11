#
#  Copyright 2016 CUBRID Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#

project(test_stream_pipeline)

set(SRC
    ${PARALLEL_QUERY_DIR}/px_stream_channel.hpp
    ${PARALLEL_QUERY_DIR}/px_stream_channel.cpp
    ${PARALLEL_QUERY_DIR}/px_stream_source.hpp
    ${PARALLEL_QUERY_DIR}/px_stream_source.cpp
    ${PARALLEL_QUERY_DIR}/px_stream_pipeline.hpp
    ${PARALLEL_QUERY_DIR}/px_stream_pipeline.cpp
    ${PARALLEL_QUERY_DIR}/px_stream_policy.hpp
    src/test_stream_pipeline.cpp
)

add_executable(test_stream_pipeline ${SRC})

# The C7+C4 lifetime prototype (no real join): real pipeline + channel + source TUs,
# fake producer tasks/pool in the test.  Almost self-contained: only er_set +
# db_private_free_release are needed at link time, both stubbed inside
# test_stream_pipeline.cpp (the stubs double as the error/alloc ledgers).  No build
# mode is defined, so THREAD_ENTRY is void and memory_wrapper.hpp is inert -- no
# engine library link.  NDEBUG selects the *_release allocator names and the
# er_set-based assert_release.  px_stream_policy.cpp is NOT compiled here (it needs
# the engine worker_manager); the test exercises the shared header-inline reservation
# primitive stream_try_reserve_exact directly.
target_compile_definitions(test_stream_pipeline PRIVATE NDEBUG)

target_include_directories(test_stream_pipeline PRIVATE
    ${PARALLEL_QUERY_DIR}
    ${BASE_DIR}
)

find_package(Threads REQUIRED)
target_link_libraries(test_stream_pipeline PRIVATE Threads::Threads)
