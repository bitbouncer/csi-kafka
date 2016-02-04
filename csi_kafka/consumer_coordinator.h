#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include "lowlevel_client.h"

#pragma once
namespace csi
{
    namespace kafka
    {
        class consumer_coordinator
        {
        public:
            typedef boost::function <void(const boost::system::error_code&)>       connect_callback;
            typedef boost::function <void(rpc_result<metadata_response>)>          get_metadata_callback;
            typedef boost::function <void(rpc_result<group_coordinator_response>)> get_group_coordinator_callback;
            typedef boost::function <void(rpc_result<offset_fetch_response>)>      get_consumer_offset_callback;
            typedef boost::function <void(rpc_result<offset_commit_response>)>     commit_offset_callback;

            consumer_coordinator(boost::asio::io_service& io_service, const std::string& topic, const std::string& consumer_group, int32_t rx_timeout);
            ~consumer_coordinator();

            void                                   connect_async(const broker_address& address, int32_t timeout, connect_callback);
            boost::system::error_code              connect(const broker_address& address, int32_t timeout);

            void                                   connect_async(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout, connect_callback cb);
            boost::system::error_code              connect(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout);
            void                                   close();

            void                                   get_metadata_async(get_metadata_callback cb);
            rpc_result<metadata_response>          get_metadata();

            void                                   get_group_coordinator_async(get_group_coordinator_callback cb);
            rpc_result<group_coordinator_response> get_group_coordinator();
            void                                   get_consumer_offset_async(int32_t partition, get_consumer_offset_callback);
            rpc_result<offset_fetch_response>      get_consumer_offset(int32_t partition);
            void                                   commit_consumer_offset_async(int32_t consumer_group_generation_id, const std::string& consumer_id, int32_t partition, int64_t offset, const std::string& metadata, commit_offset_callback);
            rpc_result<offset_commit_response>     commit_consumer_offset(int32_t consumer_group_generation_id, const std::string& consumer_id, int32_t partition, int64_t offset, const std::string& metadata);

            inline bool                            is_connected() const              { return _client.is_connected(); }
            inline bool                            is_connection_in_progress() const { return _client.is_connection_in_progress(); }
            //int32_t                                partition() const                 { return _partition; }
            const std::string&                     topic() const                     { return _topic; }
            const std::string&                     consumer_group() const            { return _consumer_group; }
        protected:
            boost::asio::io_service&        _ios;
            csi::kafka::lowlevel_client     _client;
            const std::string               _topic;
            const std::string               _consumer_group;
            int32_t                         _rx_timeout;
            bool                            _rx_in_progress;
            //const int32_t                   _partition;
            bool                            _transient_failure;
        };
    }
};