#ifdef WIN32 
#define BOOST_ASIO_ERROR_CATEGORY_NOEXCEPT noexcept(true)
#endif

#include "kafka.h"

namespace csi {
  namespace kafka {
    std::string to_string(const broker_address& ba) {
      std::string str = ba.host_name + ":" + std::to_string(ba.port);
      return str;
    }
  };
};

