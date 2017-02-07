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

std::vector<broker_address> string_to_brokers(std::string s) {
  int32_t kafka_port = 9092;
  std::vector<csi::kafka::broker_address> kafka_brokers;
  size_t last_colon = s.find_last_of(':');
  if (last_colon != std::string::npos)
    kafka_port = atoi(s.substr(last_colon + 1).c_str());
  s = s.substr(0, last_colon);

  // now find the brokers...
  size_t last_separator = s.find_last_of(',');
  while (last_separator != std::string::npos) {
    std::string host = s.substr(last_separator + 1);
    kafka_brokers.push_back(csi::kafka::broker_address(host, kafka_port));
    s = s.substr(0, last_separator);
    last_separator = s.find_last_of(',');
  }
  kafka_brokers.push_back(csi::kafka::broker_address(s, kafka_port));
  return kafka_brokers;
}
};
};

