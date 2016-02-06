#include <memory>
#include <boost/array.hpp>
#include <boost/function.hpp>
#include <boost/system/error_code.hpp>

#pragma once

namespace csi {
  namespace kafka {
    class basic_call_context {
    public:
      basic_call_context() : _tx_size(0), _rx_size(0), _rx_cursor(0), _expecting_reply(true) {}

      enum { MAX_BUFFER_SIZE = 1024 * 1024 };
      typedef boost::function <void(const boost::system::error_code&, std::shared_ptr<basic_call_context>)>	callback;
      typedef std::shared_ptr<basic_call_context>					                                            handle;

      boost::array<uint8_t, MAX_BUFFER_SIZE>  _tx_buffer;
      size_t                                  _tx_size;
      boost::array<uint8_t, MAX_BUFFER_SIZE>  _rx_buffer;
      size_t                                  _rx_size;
      size_t                                  _rx_cursor;
      bool                                    _expecting_reply;
      callback                                _callback;
    };
  };
};