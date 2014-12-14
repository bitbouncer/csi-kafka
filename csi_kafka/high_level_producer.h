#include <csi_kafka/low_level/producer.h>
#pragma once

namespace csi
{
    namespace kafka
    {
        class highlevel_producer
        {
        public:
            typedef boost::function <void(const boost::system::error_code&)> connect_callback;
            highlevel_producer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic);
            boost::system::error_code connect();

            void enqueue(std::shared_ptr<basic_message> message);


            void refresh_metadata_async();
            void close(); 
        private:
            boost::asio::io_service&             _ios;
            csi::kafka::low_level::client        _meta_client;
            std::string                          _topic_name;
            
            std::map<int, async_producer*>       _producers; // partition -> producer NOT broker -> producer
            std::map<int, broker_data>           _brokers;
            std::map<int, csi::kafka::metadata_response::topic_data::partition_data> _partitions; // partition->partition_dat

            rpc_result<metadata_response>        _metadata;
        };
    };
};
