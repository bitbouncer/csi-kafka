#include <csi_kafka/lowlevel_client.h>
#pragma once

namespace csi {
namespace kafka {
class async_metadata_client
{
  public:
  typedef std::function <void(const boost::system::error_code&)> connect_callback;
  typedef std::function <void(rpc_result<metadata_response>)>    get_metadata_callback;

  async_metadata_client(boost::asio::io_service& io_service, std::string topic);
  ~async_metadata_client();
  void                              connect_async(const std::vector<broker_address>& brokers, int32_t timeout, connect_callback cb); // should maybee an event stream that we can subcribe to like change-events. TIMEOUT
  boost::system::error_code         connect(const std::vector<broker_address>& brokers, int32_t timeout);
  void                              close();
  bool                              is_connected() const;

  void                              get_metadata_async(const std::vector<std::string>& topics, get_metadata_callback);
  rpc_result<metadata_response>     get_metadata(const std::vector<std::string>& topics);

  protected:
  void                              _connect_async_next();
  void                              handle_get_metadata(rpc_result<metadata_response> response);

  // asio callbacks
  void                              handle_connect_retry_timer(const boost::system::error_code& ec);
  void                              handle_get_metadata_timer(const boost::system::error_code& ec);

  boost::asio::io_service&                                                 _ios;
  boost::asio::deadline_timer			                                         _metadata_timer;
  boost::asio::deadline_timer			                                         _connect_retry_timer;
  boost::posix_time::time_duration	                                       _metadata_timeout;
  boost::posix_time::time_duration                                         _current_retry_timeout;
  boost::posix_time::time_duration                                         _max_retry_timeout;
  csi::kafka::lowlevel_client                                              _client;
  mutable csi::spinlock                                                    _spinlock; // protects the metadata below
  std::vector<broker_address>                                              _known_brokers;
  std::vector<broker_address>::iterator                                    _next_broker;
  connect_callback                                                         _connect_cb;
  rpc_result<metadata_response>                                            _metadata;
  std::string                                                              _topic;
  std::map<int, broker_data>                                               _broker2brokers;
  std::map<int, csi::kafka::metadata_response::topic_data::partition_data> _partition2partitions;
};
};
};


