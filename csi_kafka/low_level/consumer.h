#include "client.h"

#pragma once
namespace csi
{
    namespace kafka
    {
        class lowlevel_consumer
        {
        public:
            typedef boost::function <void(const boost::system::error_code&)> connect_callback;
            typedef boost::function <void(csi::kafka::error_codes)> set_offset_callback;
            typedef boost::function <void(csi::kafka::error_codes, const csi::kafka::fetch_response::topic_data::partition_data&)> datastream_callback;
            typedef boost::function <void(csi::kafka::error_codes, std::shared_ptr<metadata_response>)> get_metadata_callback;

            lowlevel_consumer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic, int32_t partition);

            void                        connect_async(connect_callback cb);
            boost::system::error_code   connect();
            void                        set_offset_async(int64_t start_time, set_offset_callback cb);
            csi::kafka::error_codes     set_offset      (int64_t start_time);

            void                        get_next_data_async(datastream_callback cb);
            void                        open_stream(datastream_callback cb);
            void                        get_metadata_async(get_metadata_callback cb);
            inline bool                 is_connected() const { return _client.is_connected(); }

        protected:
            boost::asio::io_service&             _ios;
            csi::kafka::low_level::client        _client;
            std::string                          _topic_name;
            int32_t                              _partition_id;
            int64_t                              _next_offset;
        };
    }
};