#include "pl_result_cache.hpp"
#include "dbtype.h"
#include "object_representation.h"
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
    m_max_memory = 16 * 1024 * 1024; // 16MB
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
	    value_hash = 0;
	    break;
	  case DB_TYPE_INTEGER:
	    value_hash = std::hash<int> {} (db_get_int (&value));
	    break;
	  case DB_TYPE_SHORT:
	    value_hash = std::hash<short> {} (db_get_short (&value));
	    break;
	  case DB_TYPE_BIGINT:
	  {
	    DB_BIGINT bigint = db_get_bigint (&value);
	    unsigned int x = bigint >> 32;
	    unsigned int y = (unsigned int) bigint;
	    value_hash = x ^ y;
	  }
	  break;
	  case DB_TYPE_FLOAT:
	    value_hash = std::hash<float> {} (db_get_float (&value));
	    break;
	  case DB_TYPE_DOUBLE:
	    value_hash = std::hash<double> {} (db_get_double (&value));
	    break;
	  case DB_TYPE_NUMERIC:
	    value_hash = std::hash<DB_C_NUMERIC> {} (db_get_numeric (&value));
	    break;
	  case DB_TYPE_CHAR:
	  case DB_TYPE_NCHAR:
	  case DB_TYPE_VARCHAR:
	  case DB_TYPE_VARNCHAR:
	    value_hash = std::hash<std::string> {} (db_get_string (&value));
	    break;
	  case DB_TYPE_TIME:
	    value_hash = std::hash<unsigned int> {} (*db_get_time (&value));
	    break;
	  case DB_TYPE_TIMESTAMP:
	  case DB_TYPE_TIMESTAMPLTZ:
	    value_hash = std::hash<DB_TIMESTAMP> {} (*db_get_timestamp (&value));
	    break;
	  case DB_TYPE_TIMESTAMPTZ:
	    value_hash = std::hash<DB_TIMESTAMP> {} (db_get_timestamptz (&value)->timestamp);
	    break;
	  case DB_TYPE_DATETIME:
	  case DB_TYPE_DATETIMELTZ:
	  {
	    DB_DATETIME *datetime = db_get_datetime (&value);
	    value_hash = std::hash<unsigned int> {} (datetime->date ^ datetime->time);
	  }
	  break;
	  case DB_TYPE_DATETIMETZ:
	  {
	    DB_DATETIMETZ *dt_tz = db_get_datetimetz (&value);
	    value_hash = std::hash<unsigned int> {} (dt_tz->datetime.date ^ dt_tz->datetime.time);
	  }
	  break;
	  case DB_TYPE_DATE:
	    value_hash = std::hash<unsigned int> {} (*db_get_date (&value));
	    break;
	  case DB_TYPE_MONETARY:
	    value_hash = std::hash<double> {} (db_get_monetary (&value)->amount);
	    break;
	  case DB_TYPE_SET:
	  case DB_TYPE_MULTISET:
	  case DB_TYPE_SEQUENCE:
	  {
	    DB_SET *set = db_get_set (&value);
	    DB_VALUE temp;
	    value_hash = 0;
	    if (db_set_get (set, 0, &temp) == NO_ERROR)
	      {
		result_cache::key_hash hasher;
		result_cache::key temp_key;
		temp_key.push_back (temp);
		value_hash = hasher (temp_key);
		db_value_clear (&temp);
	      }
	  }
	  break;
	  case DB_TYPE_OBJECT:
	    value_hash = std::hash<DB_OBJECT *> {} (db_get_object (&value));
	    break;
	  case DB_TYPE_OID:
	  {
	    OID *oid = db_get_oid (&value);
	    value_hash = std::hash<unsigned int> {} (oid->volid ^ oid->pageid ^ oid->slotid);
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