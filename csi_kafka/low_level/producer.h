#include "client.h"
#include <deque>

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

            producer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic, int32_t partition);

            void connect_async(connect_callback cb);
            boost::system::error_code connect();
            void close();
            //void close_async();

            void send_async(int32_t required_acks, int32_t timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, send_callback);
            
            inline bool is_connected() const    { return _client.is_connected(); }
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
            typedef boost::function <void(rpc_result<csi::kafka::produce_response>)> send_callback;

            async_producer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic, int32_t partition);

            void connect_async(connect_callback cb);
            boost::system::error_code connect();
            void close();
            void enqueue(std::shared_ptr<basic_message> message);

            //void close_async();

            inline bool is_connected() const    { return _client.is_connected(); }
            int32_t partition() const           { return _partition_id; }
            const std::string& topic() const    { return _topic_name; }

            size_t items_in_queue() const { return _tx_queue.size(); } // no lock but should not matter
            size_t bytes_in_queue() const { return _tx_queue_byte_size; } // no lock but should not matter

        protected:
            void _try_send(); // gets posted from enqueue so actual call comes from correct thread

            //void send_async(const std::vector<basic_message>& v, int32_t correlation_id, send_callback);
            //void send_async(const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, send_callback cb);

            boost::asio::io_service&                   _ios;
            csi::kafka::low_level::client              _client;
            const std::string                          _topic_name;
            const int32_t                              _partition_id;
            //TX queue
            csi::kafka::spinlock                       _spinlock;
            std::deque<std::shared_ptr<basic_message>> _tx_queue;
            size_t                                     _tx_queue_byte_size;
            bool                                       _tx_in_progress;
            int32_t                                    _required_acks;
            int32_t                                    _tx_timeout;
        };
    }
};