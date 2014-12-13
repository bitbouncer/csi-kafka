#include "client.h"

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

            void send_async(int32_t required_acks, int32_t timeout, const std::vector<basic_message>& v, int32_t correlation_id, send_callback);
            
            inline bool is_connected() const    { return _client.is_connected(); }
            int32_t partition() const           { return _partition_id; }
            const std::string& topic() const    { return _topic_name; }

        protected:
            boost::asio::io_service&             _ios;
            csi::kafka::low_level::client        _client;
            const std::string                    _topic_name;
            const int32_t                        _partition_id;
        };
    }
};