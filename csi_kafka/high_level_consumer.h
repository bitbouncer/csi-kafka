#include <csi_kafka/low_level/consumer.h>

namespace csi
{
    namespace kafka
    {
        class highlevel_consumer
        {
        public:
            typedef boost::function <void(const boost::system::error_code&)> connect_callback;

            highlevel_consumer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic); 
            boost::system::error_code connect();


            void refresh_metadata_async();


            boost::asio::io_service&             _ios;
            csi::kafka::low_level::client        _meta_client;
            std::string                          _topic_name;
            std::vector<lowlevel_consumer*>      _consumers;
            std::shared_ptr<metadata_response>   _metadata;


        };
    };
};

//std::vector<csi::kafka::broker_data> _brokers;