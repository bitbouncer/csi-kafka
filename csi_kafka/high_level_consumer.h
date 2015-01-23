#include <map>
#include <csi_kafka/low_level/consumer.h>
#include <csi_kafka/internal/async_metadata_client.h>

#pragma once

namespace csi
{
    namespace kafka
    {
        class highlevel_consumer
        {
        public:
            struct metrics
            {
                int         partition;
                std::string host;
                int         port;
                //size_t      msg_in_queue;
                //size_t      bytes_in_queue;
                uint32_t    rx_kb_sec;
                uint32_t    rx_msg_sec;
                double      rx_roundtrip;
            };

            typedef boost::function <void(const boost::system::error_code&)> connect_callback;
            typedef boost::function <void(const boost::system::error_code& ec1, csi::kafka::error_codes ec2, const csi::kafka::fetch_response::topic_data::partition_data&)> datastream_callback;
        
            highlevel_consumer(boost::asio::io_service& io_service, const std::string& topic, int32_t rx_timeout, int32_t max_packet_size = -1);
            ~highlevel_consumer(); 

            void set_offset(int64_t start_time);
            void connect_async(const std::vector<broker_address>& brokers); // callback?
            //void refresh_metadata_async();


            void close();
            void stream_async(datastream_callback cb);

            std::vector<metrics> get_metrics() const;

        private:
            void handle_timer(const boost::system::error_code& ec);
            void _try_connect_brokers();

            boost::asio::io_service&             _ios;
            boost::asio::deadline_timer			 _timer;
            boost::posix_time::time_duration	 _timeout;

            std::string                          _topic;
            int32_t                              _rx_timeout;
            int32_t                              _max_packet_size;
            std::map<int, lowlevel_consumer2*>   _partition2consumers;

            // CLUSTER METADATA
            csi::kafka::async_metadata_client                                        _meta_client;
            csi::kafka::spinlock                                                     _spinlock; // protects the metadata below
            //rpc_result<metadata_response>                                            _metadata;
            std::map<int, broker_data>                                               _broker2brokers;
            std::map<int, csi::kafka::metadata_response::topic_data::partition_data> _partition2partitions; // partition->partition_dat
        };
    };
};
