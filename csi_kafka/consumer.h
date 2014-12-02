#include "low_level/client.h"

#pragma once
namespace csi
{
    namespace kafka
    {
        class consumer
        {
        public:
            enum state_t { IDLE, CONNETING_TO_CLUSTER, GETTING_METADATA, CONNECTING_TO_PARTION_LEADER, READY };

            typedef boost::function <void(const csi::kafka::fetch_response::topic_data::partition_data&)> datastream_callback;

            consumer(boost::asio::io_service& io_service, const std::string& hostname, const std::string& port, const std::string& topic, int32_t partition);

            void start(int64_t start_time, datastream_callback cb);

        protected:
            void _on_retry_timer(const boost::system::error_code& ec);
            void _on_cluster_connect(const boost::system::error_code& ec);
            void _on_metadata_request(csi::kafka::error_codes ec, csi::kafka::basic_call_context::handle handle);
            void _on_leader_connect(const boost::system::error_code& ec);
            void _on_offset_request(csi::kafka::error_codes ec, csi::kafka::basic_call_context::handle handle);
            void _on_fetch_data(csi::kafka::error_codes ec, csi::kafka::basic_call_context::handle handle);

            boost::asio::io_service&             _ios;
            csi::kafka::low_level::client        _client;
            std::string                          _hostname; // shoulkd really be a vector of known hosts in the cluster...
            std::string                          _port;
            std::string                          _topic_name;
            int32_t                              _partition_id;
            state_t                              _state;
            datastream_callback                  _datastream_callback;
            int64_t                              _next_offset;
            int32_t                              _partition_leader;
            std::vector<csi::kafka::broker_data> _brokers;
            
            
            int64_t                             _start_point_in_time;
        };
    }
};