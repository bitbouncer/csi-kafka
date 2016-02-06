#include <map>
#include <functional>
#include <memory>
#include <boost/noncopyable.hpp>
#include "spinlock.h"

#pragma once

namespace csi {
  namespace kafka {
    template<class key_type, class value_type, typename key_compare = std::less<key_type>>
    class table : public boost::noncopyable {
    public:
      typedef typename std::map<key_type, std::shared_ptr<value_type>, key_compare>::iterator	       iterator;
      typedef typename std::map<key_type, std::shared_ptr<value_type>, key_compare>::const_iterator  const_iterator;

      void put(const key_type& key, std::shared_ptr<value_type> v) {
        csi::kafka::spinlock::scoped_lock xx(_spinlock);
        _data[key] = v;
      }

      std::shared_ptr<value_type> get(const key_type& key) const {
        csi::kafka::spinlock::scoped_lock xx(_spinlock);
        const_iterator item = _data.find(key);
        if(item != _data.end())
          return item->second;
        return std::shared_ptr<value_type>(NULL);
      }

      size_t size() const { return _data.size(); } // skip the lock - it should be ok anyway

    private:
      mutable csi::kafka::spinlock                                 _spinlock;
      std::map<key_type, std::shared_ptr<value_type>, key_compare> _data;
    };
  }
}