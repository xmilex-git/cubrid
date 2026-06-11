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

project(test_stream_source)

set(SRC
    ${PARALLEL_QUERY_DIR}/px_stream_channel.hpp
    ${PARALLEL_QUERY_DIR}/px_stream_channel.cpp
    ${PARALLEL_QUERY_DIR}/px_stream_source.hpp
    ${PARALLEL_QUERY_DIR}/px_stream_source.cpp
    src/test_stream_source.cpp
)

add_executable(test_stream_source ${SRC})

# Almost self-contained: px_stream_source.cpp needs only er_set + db_private_free at link
# time, both stubbed inside test_stream_source.cpp (the stub doubles as the alloc/er
# ledger).  No build mode is defined, so THREAD_ENTRY is void and memory_wrapper.hpp is
# inert -- no engine library link.  NDEBUG makes assert_release take its release er_set
# path so the R4 reset guard and the fan-out refusal are testable (in debug builds they
# abort by design; the engine-flag harness covers that side).
target_compile_definitions(test_stream_source PRIVATE NDEBUG)

target_include_directories(test_stream_source PRIVATE
    ${PARALLEL_QUERY_DIR}
    ${BASE_DIR}
)

find_package(Threads REQUIRED)
target_link_libraries(test_stream_source PRIVATE Threads::Threads)
