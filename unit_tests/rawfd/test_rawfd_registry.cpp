/*
 * Copyright 2008 Search Solution Corporation
 * Copyright 2016 CUBRID Corporation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "temp_page_store.hpp"

#include <cstdio>

namespace
{
  struct rawfd_case
  {
    const char *name;
    int (*run) () noexcept;
  };
}

int
main (int, char **)
{
  const rawfd_case cases[] =
  {
    { "T1 hash-collision exact-key", temp_page_store::rawfd_test_t1_hash_collision },
    { "T2 ABA validation", temp_page_store::rawfd_test_t2_aba_validation },
    { "T3 bounded retry", temp_page_store::rawfd_test_t3_retry_bound },
    { "T4 m_destroyed hot-path gate", temp_page_store::rawfd_test_t4_destroyed_gate },
    { "T5 purge secondary-before-free", temp_page_store::rawfd_test_t5_purge_order }
  };

  bool all_passed = true;
  for (const rawfd_case &test_case : cases)
    {
      const int error = test_case.run ();
      if (error == 0)
	{
	  std::printf ("%s PASS\n", test_case.name);
	}
      else
	{
	  std::printf ("%s FAIL (%d)\n", test_case.name, error);
	  all_passed = false;
	}
    }

  if (all_passed)
    {
      std::printf ("ALL TESTS PASSED\n");
      return 0;
    }

  return 1;
}
