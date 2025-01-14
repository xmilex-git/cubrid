#ifndef _PL_RESULT_CACHE_HPP_
#define _PL_RESULT_CACHE_HPP_

#include <vector>
#include <unordered_map>
#include <functional>
#include "dbtype.h"

namespace cubpl
{
  class result_cache
  {
    public:
      result_cache () = default;
      ~result_cache () = default;

      class key : public std::vector<DB_VALUE>
      {
	public:
	  bool operator== (const key &other) const;
	  ~key();
      };

      struct key_hash
      {
	std::size_t operator() (const key &k) const;
      };

      class value : public DB_VALUE
      {
	public:
	  ~value();
      };

      void put (key args, DB_VALUE &result);
      bool get (const key &args, DB_VALUE &result);
      static key make_key (const std::vector<std::reference_wrapper<DB_VALUE>> &args);

    private:
      std::unordered_map<key, value, key_hash> map;
  };
}
#endif