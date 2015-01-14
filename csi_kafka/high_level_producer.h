#include <csi_kafka/low_level/producer.h>
#pragma once

namespace csi
{
    namespace kafka
    {

        class highlevel_producer
        {
        public:
            struct metrics
            {
                int         partition;
                std::string host;
                int         port;
                size_t      msg_in_queue;
                size_t      bytes_in_queue;
                uint32_t    tx_kb_sec;
                uint32_t    tx_msg_sec;
                double      tx_roundtrip;
            };

            typedef boost::function <void(const boost::system::error_code&)> connect_callback;
            typedef boost::function <void()>                                 tx_ack_callback;

            highlevel_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t required_acks, int32_t tx_timeout, int32_t max_packet_size=-1);
            ~highlevel_producer();
            boost::system::error_code connect(const boost::asio::ip::tcp::resolver::query& query);
            void send_async(std::shared_ptr<basic_message> message, tx_ack_callback = NULL);
            void send_async(std::vector<std::shared_ptr<basic_message>>& messages, tx_ack_callback = NULL);
            void close(); 

            std::vector<metrics> get_metrics() const;

        private:
            // asio callbacks
            void handle_timer(const boost::system::error_code& ec);
            void _try_connect_brokers();
            boost::asio::io_service&                                                 _ios;
            std::string                                                              _topic;
            int32_t                                                                  _required_acks;
            int32_t                                                                  _tx_timeout;
            int32_t                                                                  _max_packet_size;

            std::map<int, async_producer*>                                           _partition2producers;

            boost::asio::deadline_timer			                                     _timer;
            boost::posix_time::time_duration	                                     _timeout;

            // CLUSTER METADATA
            csi::kafka::low_level::client                                            _meta_client;
            csi::kafka::spinlock                                                     _spinlock; // protects the metadata below
            rpc_result<metadata_response>                                            _metadata;
            std::map<int, broker_data>                                               _broker2brokers;
            std::map<int, csi::kafka::metadata_response::topic_data::partition_data> _partition2partitions;
        };
    };
};
