#include <map>
#include <csi_kafka/low_level/consumer.h>

#pragma once

namespace csi
{
    namespace kafka
    {
        class highlevel_consumer
        {
        public:
            typedef boost::function <void(const boost::system::error_code&)> connect_callback;
            highlevel_consumer(boost::asio::io_service& io_service, const std::string& topic); 
            boost::system::error_code connect(const boost::asio::ip::tcp::resolver::query& query);
            void refresh_metadata_async();
        private:
            void _try_connect_brokers();

            boost::asio::io_service&             _ios;
            std::string                          _topic;
            std::map<int, lowlevel_consumer*>    _consumers;

            // CLUSTER METADATA
            csi::kafka::low_level::client                                            _meta_client;
            csi::kafka::spinlock                                                     _spinlock; // protects the metadata below
            rpc_result<metadata_response>                                            _metadata;
            std::map<int, broker_data>                                               _broker2brokers;
            std::map<int, csi::kafka::metadata_response::topic_data::partition_data> _partition2partitions; // partition->partition_dat
        };
    };
};
