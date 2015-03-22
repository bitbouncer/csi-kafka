#include "lowlevel_client.h"
#include <deque>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>

#pragma once
namespace csi
{
    namespace kafka
    {
        class lowlevel_producer  // -> lowlevel_producer
        {
        public:
            typedef boost::function <void(const boost::system::error_code&)>                                connect_callback;
            typedef boost::function <void(rpc_result<csi::kafka::produce_response>)>                        send_callback;
            typedef boost::function <void(csi::kafka::error_codes, std::shared_ptr<metadata_response>)>     get_metadata_callback;

            lowlevel_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition);

            void                              connect_async(const broker_address& address, int32_t timeout, connect_callback);
            boost::system::error_code         connect(const broker_address& address, int32_t timeout);

            void                              connect_async(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout, connect_callback cb);
            boost::system::error_code         connect(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout);
            void close();
            //void close_async();

            void send_async(int32_t required_acks, int32_t timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, send_callback);
            
            inline bool is_connected() const    { return _client.is_connected(); }
            inline bool is_connection_in_progress() const { return _client.is_connection_in_progress(); }
            int32_t partition() const           { return _partition_id; }
            const std::string& topic() const    { return _topic; }

        protected:
            boost::asio::io_service&             _ios;
            csi::kafka::lowlevel_client          _client;
            const std::string                    _topic;
            const int32_t                        _partition_id;
        };
    }
};