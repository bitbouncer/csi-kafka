#include "low_level/client.h"

#pragma once
namespace csi
{
    namespace kafka
    {
        class producer
        {
        public:
            enum state_t { IDLE, CONNETING_TO_CLUSTER, GETTING_METADATA, CONNECTING_TO_PARTION_LEADER, READY };

            typedef boost::function <void(csi::kafka::error_codes error, std::shared_ptr<csi::kafka::produce_response>)> callback;

            producer(boost::asio::io_service& io_service, const std::string& hostname, const std::string& port, const std::string& topic, int32_t partition);

            void start();
            void send_async(int32_t required_acks, int32_t timeout, const std::vector<basic_message>& v, int32_t correlation_id, callback cb);
            //std::shared_ptr<csi::kafka::produce_response> send_sync(int32_t required_acks, int32_t timeout, const std::vector<basic_message>& v, int32_t correlation_id, callback cb);

        protected:
            void _on_retry_timer(const boost::system::error_code& ec);
            void _on_cluster_connect(const boost::system::error_code& ec);
            void _on_metadata_request(csi::kafka::error_codes ec, csi::kafka::basic_call_context::handle handle);
            void _on_leader_connect(const boost::system::error_code& ec);

            boost::asio::io_service&             _ios;
            csi::kafka::low_level::client        _client;
            std::string                          _hostname; // should really be a vector of known hosts in the cluster...
            std::string                          _port;
            std::string                          _topic_name;
            int32_t                              _partition_id;
            state_t                              _state;
            int32_t                              _partition_leader;
            std::vector<csi::kafka::broker_data> _brokers;
        };
    }
};