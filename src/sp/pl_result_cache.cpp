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
 *
 */

/*
 * pl_result_cache.cpp
 */

#ident "$Id$"
#include "pl_result_cache.hpp"
#include "dbtype.h"
#include "object_representation.h"
#include "memory_hash.h"
#include "system_parameter.h"
// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"
namespace cubpl
{
  std::size_t result_cache::key::get_memory_size() const
  {
    std::size_t total_size = sizeof (key);
    for (auto &val : *this)
      {
	total_size += or_db_value_size ((DB_VALUE *)&val);
      }
    return total_size;
  }

  std::size_t result_cache::value::get_memory_size() const
  {
    return or_db_value_size ((DB_VALUE *)this);
  }

  result_cache::result_cache()
  {
    m_total_memory = sizeof (result_cache);
    m_max_memory = static_cast<std::size_t> (prm_get_bigint_value (PRM_ID_MAX_SP_CACHE_SIZE));
    m_is_enabled = true;
    m_cache_hit = 0;
    m_cache_miss = 0;
  }

  bool result_cache::key::operator== (const key &other) const
  {
    if (this->size() != other.size())
      {
	return false;
      }

    for (size_t i = 0; i < this->size(); i++)
      {
	if (db_value_compare (& (*this)[i], &other[i]) != DB_EQ)
	  {
	    return false;
	  }
      }
    return true;
  }

  void result_cache::put (key args, DB_VALUE &result)
  {
    if (!m_is_enabled)
      {
	return;
      }
    std::size_t new_value_size = args.get_memory_size() + or_db_value_size ((DB_VALUE *)&result) + sizeof (
					 void *); /* bucket size */
    if (m_total_memory + new_value_size > m_max_memory)
      {
	m_is_enabled = false;
	return;
      }
    else if (m_total_memory + new_value_size > m_max_memory / 2)
      {
	if (m_cache_hit < 9 * m_cache_miss)
	  {
	    m_is_enabled = false;
	    return;
	  }
      }
    value new_value;
    db_value_clone (&result, &new_value);
    map.emplace (std::move (args), std::move (new_value));
    m_total_memory += new_value_size;
  }

  bool result_cache::get (const key &args, DB_VALUE &result)
  {
    auto it = map.find (args);
    if (it != map.end())
      {
	db_value_clone (&it->second, &result);
	m_cache_hit++;
	return true;
      }
    m_cache_miss++;
    return false;
  }

  std::size_t result_cache::key_hash::operator() (const key &k) const
  {
    std::size_t hash_val = 0;

    for (auto &value : k)
      {
	std::size_t value_hash = 0;

	if (DB_IS_NULL (&value))
	  {
	    continue;
	  }

	switch (DB_VALUE_TYPE (&value))
	  {
	  case DB_TYPE_NULL:
	  case DB_TYPE_INTEGER:
	  case DB_TYPE_SHORT:
	  case DB_TYPE_BIGINT:
	  case DB_TYPE_FLOAT:
	  case DB_TYPE_DOUBLE:
	  case DB_TYPE_NUMERIC:
	  case DB_TYPE_CHAR:
	  case DB_TYPE_NCHAR:
	  case DB_TYPE_VARCHAR:
	  case DB_TYPE_VARNCHAR:
	  case DB_TYPE_TIME:
	  case DB_TYPE_TIMESTAMP:
	  case DB_TYPE_TIMESTAMPLTZ:
	  case DB_TYPE_TIMESTAMPTZ:
	  case DB_TYPE_DATETIME:
	  case DB_TYPE_DATETIMELTZ:
	  case DB_TYPE_DATETIMETZ:
	  case DB_TYPE_DATE:
	  case DB_TYPE_MONETARY:
	  case DB_TYPE_SET:
	  case DB_TYPE_MULTISET:
	  case DB_TYPE_SEQUENCE:
	  case DB_TYPE_OBJECT:
	  case DB_TYPE_OID:
	  {
	    value_hash = static_cast<std::size_t> (mht_valhash (&value, INT_MAX));
	  }
	  break;
	  default:
	    assert (false);
	    break;
	  }

	hash_val = hash_val * 31 + value_hash;
      }

    return hash_val;
  }

  result_cache::key result_cache::make_key (const std::vector<std::reference_wrapper<DB_VALUE>> &args)
  {
    key new_key;
    new_key.reserve (args.size());

    for (auto &ref : args)
      {
	DB_VALUE new_value;
	db_value_clone ((DB_VALUE *)&ref.get(), &new_value);
	new_key.push_back (std::move (new_value));
      }

    return new_key;
  }

  result_cache::key::~key()
  {
    for (auto &value : *this)
      {
	db_value_clear (&value);
      }
  }

  result_cache::value::~value()
  {
    db_value_clear (this);
  }
}