#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include "lowlevel_client.h"

#pragma once
namespace csi {
  namespace kafka {
    class consumer_coordinator {
    public:
      typedef std::function <void(const boost::system::error_code&)>   connect_callback;
      typedef std::function <void(rpc_result<offset_fetch_response>)>  get_consumer_offset_callback;
      typedef std::function <void(rpc_result<offset_commit_response>)> commit_offset_callback;

      consumer_coordinator(boost::asio::io_service& io_service, const std::string& consumer_group);
      ~consumer_coordinator();

      void                                   connect_async(const std::vector<broker_address>& brokers, int32_t timeout, connect_callback cb);
      boost::system::error_code              connect(const std::vector<broker_address>& brokers, int32_t timeout);
      boost::system::error_code              connect(std::string brokers, int32_t timeout);

      void                                   close();
      void                                   get_consumer_offset_async(std::string topic, get_consumer_offset_callback);
      rpc_result<offset_fetch_response>      get_consumer_offset(std::string topic);
      void                                   get_consumer_offset_async(std::string topic, int32_t partition, get_consumer_offset_callback);
      rpc_result<offset_fetch_response>      get_consumer_offset(std::string topic, int32_t partition);

      void                                   commit_consumer_offset_async(int32_t consumer_group_generation_id, const std::string& consumer_id, std::string topic, const std::vector<topic_offset>&, const std::string& metadata, commit_offset_callback);
      rpc_result<offset_commit_response>     commit_consumer_offset(int32_t consumer_group_generation_id, const std::string& consumer_id, std::string topic, const std::vector<topic_offset>&, const std::string& metadata);

      void                                   commit_consumer_offset_async(int32_t consumer_group_generation_id, const std::string& consumer_id, std::string topic, const std::map<int32_t, int64_t>&, const std::string& metadata, commit_offset_callback);
      rpc_result<offset_commit_response>     commit_consumer_offset(int32_t consumer_group_generation_id, const std::string& consumer_id, std::string topic, const std::map<int32_t, int64_t>&, const std::string& metadata);


      inline bool                            is_connected() const { return _client.is_connected(); }
      inline bool                            is_connection_in_progress() const { return _client.is_connection_in_progress(); }
      const std::string&                     consumer_group() const { return _consumer_group; }
    protected:
      boost::asio::io_service&    _ios;
      csi::kafka::lowlevel_client _client;
      const std::string           _consumer_group;
      std::vector<broker_address> _initial_brokers;
    };

    std::map<int32_t, int64_t> parse(rpc_result<offset_fetch_response> result, std::string topic, int32_t& ec);
    //std::vector<topic_offset> parse(rpc_result<offset_fetch_response>, std::string topic, int32_t& ec);     
  }
};