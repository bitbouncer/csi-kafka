#include <vector>
#include <boost/function.hpp>
#include <boost/asio.hpp>

#pragma once

namespace csi {
  namespace async {
    template <typename Value>
    class destructor_callback {
    public:
      destructor_callback(boost::function <void(Value& val)>  callback) : _cb(callback) {}
      ~destructor_callback() { _cb(_val); }
      Value& value() { return _val; }
      const Value& value() const { return _val; }
    private:
      boost::function <void(Value& ec)> _cb;
      Value _val;
    };

    typedef boost::function <void(std::vector<boost::system::error_code>)>  async_vcallback;
    typedef boost::function <void(const boost::system::error_code&)>        async_callback;
    typedef boost::function <void(async_callback)>                          async_function;

    template<typename RAIter>
    void waterfall(RAIter begin, RAIter end, async_callback cb) {
      if(begin == end) {
        cb(make_error_code(boost::system::errc::success));
        return;
      }
      (*begin)([begin, end, cb](const boost::system::error_code& ec) {
        if(!ec)
          waterfall(begin + 1, end, cb);
        else
          cb(ec); // add iterator here...
      });
    }

    inline void waterfall(const std::vector<async_function>& v, async_callback cb) {
      waterfall(v.begin(), v.end(), cb);
    }

    class parallel_result {
    public:
      parallel_result(size_t nr_of_tasks, async_vcallback callback) : _cb(callback), _result(nr_of_tasks, make_error_code(boost::system::errc::success)) {}
      ~parallel_result() { _cb(_result); }
      boost::system::error_code& operator[](size_t i) { return _result[i]; }
      const boost::system::error_code& operator[](size_t i) const { return _result[i]; }
    private:
      async_vcallback                        _cb;
      std::vector<boost::system::error_code> _result;
    };

    template<typename RAIter>
    void parallel(RAIter begin, RAIter end, async_vcallback cb) {
      std::shared_ptr<parallel_result>     result(new parallel_result(end - begin, cb));
      size_t index = 0;
      for(RAIter i = begin; i != end; ++i, ++index) {
        (*i)([i, result, index](const boost::system::error_code& ec) {
          (*result)[index] = ec;
        });
      }
    }

    inline void parallel(const std::vector<async_function>& v, async_vcallback cb) {
      parallel(v.begin(), v.end(), cb);
    }

    template<typename RAIter>
    void first_success(RAIter begin, RAIter end, async_callback cb) {
      if(begin == end) {
        cb(make_error_code(boost::system::errc::success));
        return;
      }
      (*begin)([begin, end, cb](const boost::system::error_code& ec) {
        if(ec) {
          auto next = begin + 1;
          if(next == end)
            cb(ec);
          else
            first_success(next, end, cb);
        } else
          cb(ec); // add iterator here...
      });
    }

    inline void first_success(const std::vector<async_function>& v, async_callback cb) {
      first_success(v.begin(), v.end(), cb);
    }
  };
};
