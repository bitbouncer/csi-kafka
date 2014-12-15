#include "client.h"
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
        class producer
        {
        public:
            typedef boost::function <void(const boost::system::error_code&)>                                connect_callback;
            typedef boost::function <void(rpc_result<csi::kafka::produce_response>)>                        send_callback;
            typedef boost::function <void(csi::kafka::error_codes, std::shared_ptr<metadata_response>)>     get_metadata_callback;

            producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition);

            void connect_async(const boost::asio::ip::tcp::resolver::query& query, connect_callback cb);
            boost::system::error_code connect(const boost::asio::ip::tcp::resolver::query& query);
            void close();
            //void close_async();

            void send_async(int32_t required_acks, int32_t timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, send_callback);
            
            inline bool is_connected() const    { return _client.is_connected(); }
            inline bool is_connection_in_progress() const { return _client.is_connection_in_progress(); }
            int32_t partition() const           { return _partition_id; }
            const std::string& topic() const    { return _topic_name; }

        protected:
            boost::asio::io_service&             _ios;
            csi::kafka::low_level::client        _client;
            const std::string                    _topic_name;
            const int32_t                        _partition_id;
        };

        class async_producer
        {
        public:
            typedef boost::function <void(const boost::system::error_code&)>         connect_callback;
            typedef boost::function <void()>                                         tx_ack_callback;
            //typedef boost::function <void(rpc_result<csi::kafka::produce_response>)> send_callback;

            async_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition, int32_t required_acks, int32_t timeout, int32_t max_packet_size=-1);
            ~async_producer();

            void connect_async(const boost::asio::ip::tcp::resolver::query& query, connect_callback cb);
            boost::system::error_code connect(const boost::asio::ip::tcp::resolver::query& query);
            void close();
            void send_async(std::shared_ptr<basic_message> message, tx_ack_callback = NULL);

            //void wait_lowwater_async(int max_messages, lowwater_callback);
            //void close_async();

            inline bool is_connected() const              { return _client.is_connected(); }
            inline bool is_connection_in_progress() const { return _client.is_connection_in_progress(); }
            int32_t partition() const                     { return _partition_id; }
            const std::string& topic() const              { return _topic_name; }

            size_t items_in_queue() const { return _tx_queue.size(); } // no lock but should not matter
            size_t bytes_in_queue() const { return _tx_queue_byte_size; } // no lock but should not matter

            uint32_t metrics_kb_sec() const { return (uint32_t) boost::accumulators::rolling_mean(_metrics_tx_kb_sec); } // lock ???
            uint32_t metrics_msg_sec() const { return (uint32_t) boost::accumulators::rolling_mean(_metrics_tx_msg_sec); } // lock ???
            double   metrics_tx_roundtrip() const { return boost::accumulators::rolling_mean(_metrics_tx_roundtrip); } // lock ???

        protected:
            struct tx_item
            {
                tx_item(std::shared_ptr<basic_message> message) : msg(message) {}
                tx_item(std::shared_ptr<basic_message> message, tx_ack_callback callback) : msg(message), cb(callback) {}
                std::shared_ptr<basic_message> msg;
                tx_ack_callback                cb;
            };

            typedef boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> >   metrics_accumulator_t;

            void handle_metrics_timer(const boost::system::error_code& ec);
            void _try_send(); // gets posted from enqueue so actual call comes from correct thread

            boost::asio::io_service&                   _ios;
            csi::kafka::low_level::client              _client;
            const std::string                          _topic_name;
            const int32_t                              _partition_id;
            //TX queue
            csi::kafka::spinlock                       _spinlock;
            std::deque<tx_item>                        _tx_queue;
            size_t                                     _tx_queue_byte_size;
            bool                                       _tx_in_progress;
            int32_t                                    _required_acks;
            int32_t                                    _tx_timeout;
            int32_t                                    _max_packet_size;
            
            //METRICS
            boost::asio::deadline_timer	               _metrics_timer;
            boost::posix_time::time_duration           _metrics_timeout;
            uint64_t                                   __metrics_last_total_tx_kb;
            uint64_t                                   __metrics_last_total_tx_msg;
            uint64_t                                   _metrics_total_tx_kb;
            uint64_t                                   _metrics_total_tx_msg;
            metrics_accumulator_t                      _metrics_tx_kb_sec;
            metrics_accumulator_t                      _metrics_tx_msg_sec;
            metrics_accumulator_t                      _metrics_tx_roundtrip;
        };
    }
};