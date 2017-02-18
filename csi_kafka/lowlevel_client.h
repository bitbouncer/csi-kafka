#include <deque>
#include <future>
#include <boost/asio.hpp>
#include <csi_kafka/internal/call_context.h>
#include <csi-async/spinlock.h>
#include <csi_kafka/kafka.h>
#include <csi_kafka/kafka_error_code.h>
#include <csi_kafka/protocol_decoder.h>
#pragma once

namespace csi {
namespace kafka {
class lowlevel_client
{
  public:
  typedef std::function < void(const boost::system::error_code&)>       connect_callback;
  typedef std::function <void(rpc_result<metadata_response>)>           get_metadata_callback;
  typedef std::function <void(rpc_result<produce_response>)>            send_produce_callback;
  typedef std::function <void(rpc_result<offset_response>)>             get_offset_callback;
  typedef std::function <void(rpc_result<fetch_response>)>              fetch_callback;
  typedef std::function <void(rpc_result<group_coordinator_response>)>  get_group_coordinator_callback;
  typedef std::function <void(rpc_result<offset_commit_response>)>      commit_offset_callback;
  typedef std::function <void(rpc_result<offset_fetch_response>)>       get_consumer_offset_callback;

  enum { MAX_FETCH_SIZE = basic_call_context::MAX_BUFFER_SIZE };

  lowlevel_client(boost::asio::io_service& io_service);
  ~lowlevel_client();
  boost::asio::io_service&                       io_service() { return _io_service; }
  void                                           connect_async(const broker_address& address, int32_t timeout, connect_callback);
  boost::system::error_code                      connect(const broker_address& address, int32_t timeout);

  void                                           connect_async(const std::string& host, int32_t port, int32_t timeout, connect_callback);
  boost::system::error_code                      connect(const std::string& host, int32_t port, int32_t timeout);

  void                                           connect_async(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout, connect_callback);
  boost::system::error_code                      connect(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout);

  bool                                           close();
  bool                                           is_connected() const;
  bool                                           is_connection_in_progress() const;

  void                                           get_metadata_async(const std::vector<std::string>& topics, get_metadata_callback);
  rpc_result<metadata_response>                  get_metadata(const std::vector<std::string>& topics);

  void                                           send_produce_async(const std::string& topic, int32_t partition_id, int required_acks, int timeout, const std::vector<std::shared_ptr<basic_message>>& v, send_produce_callback);
  rpc_result<produce_response>                   send_produce(const std::string& topic, int32_t partition_id, int required_acks, int timeout, const std::vector<std::shared_ptr<basic_message>>& v);

  void                                           get_offset_async(const std::string& topic, int32_t partition, int64_t start_time, int32_t max_number_of_offsets, get_offset_callback);
  rpc_result<offset_response>                    get_offset(const std::string& topic, int32_t partition, int64_t start_time, int32_t max_number_of_offsets);

  void                                           fetch_async(const std::string& topic, const std::vector<partition_cursor>&, uint32_t max_wait_time, size_t min_bytes, size_t max_bytes, fetch_callback);
  rpc_result<fetch_response>                     fetch(const std::string& topic, const std::vector<partition_cursor>&, uint32_t max_wait_time, size_t min_bytes, size_t max_bytes);

  void                                           get_group_coordinator_async(const std::string& consumer_group, get_group_coordinator_callback);
  rpc_result<group_coordinator_response>         get_group_coordinator(const std::string& consumer_group);

  void                                           commit_consumer_offset_async(const std::string& consumer_group, int32_t consumer_group_generation_id, const std::string& consumer_id, const std::string& topic, const std::vector<topic_offset>& offsets, const std::string& metadata, commit_offset_callback);
  rpc_result<offset_commit_response>             commit_consumer_offset(const std::string& consumer_group, int32_t consumer_group_generation_id, const std::string& consumer_id, const std::string& topic, const std::vector<topic_offset>& offsets, const std::string& metadata);

  void                                           commit_consumer_offset_async(const std::string& consumer_group, int32_t consumer_group_generation_id, const std::string& consumer_id, const std::string& topic, const std::map<int32_t, int64_t>& offsets, const std::string& metadata, commit_offset_callback);
  rpc_result<offset_commit_response>             commit_consumer_offset(const std::string& consumer_group, int32_t consumer_group_generation_id, const std::string& consumer_id, const std::string& topic, const std::map<int32_t, int64_t>& offsets, const std::string& metadata);

  void                                           get_consumer_offset_async(const std::string& consumer_group, get_consumer_offset_callback);
  rpc_result<offset_fetch_response>              get_consumer_offset(const std::string& consumer_group);

  void                                           get_consumer_offset_async(const std::string& consumer_group, const std::string& topic, get_consumer_offset_callback);
  rpc_result<offset_fetch_response>              get_consumer_offset(const std::string& consumer_group, const std::string& topic);

  void                                           get_consumer_offset_async(const std::string& consumer_group, const std::string& topic, int32_t partition_id, get_consumer_offset_callback);
  rpc_result<offset_fetch_response>              get_consumer_offset(const std::string& consumer_group, const std::string& topic, int32_t partition_id);


  boost::asio::ip::tcp::endpoint                 remote_endpoint(); // error ignored - not thrown...
  boost::asio::ip::tcp::endpoint                 remote_endpoint(boost::system::error_code ec);

  protected:
  void                                           perform_async(basic_call_context::handle, basic_call_context::callback cb);
  basic_call_context::handle                     perform_sync(basic_call_context::handle, basic_call_context::callback cb);

  // asio callbacks
  void handle_timer(const boost::system::error_code& ec);
  void handle_connect_timeout(const boost::system::error_code& ec);

  void _perform(basic_call_context::handle handle); // will be called in context of worker thread

  void handle_resolve(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);
  void handle_connect(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);

  void socket_rx_cb(const boost::system::error_code& e, size_t bytes_received, basic_call_context::handle handle);
  void socket_tx_cb(const boost::system::error_code& e, basic_call_context::handle handle);

  boost::asio::io_service&	                _io_service;
  mutable csi::spinlock                     _spinlock;
  boost::asio::ip::tcp::resolver            _resolver;
  boost::asio::deadline_timer               _timer;
  boost::posix_time::time_duration          _timeout;

  boost::asio::deadline_timer               _connect_timeout_timer;
  boost::asio::ip::tcp::socket              _socket;
  std::deque<basic_call_context::handle>    _tx_queue;
  std::deque<basic_call_context::handle>    _rx_queue;
  bool                                      _connected;
  bool                                      _resolve_in_progress;
  bool                                      _connection_in_progress;
  bool                                      _tx_in_progress;
  bool                                      _rx_in_progress;
  int32_t                                   _next_correlation_id;
};
} // kafka
} // csi