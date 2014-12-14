#include <deque>
#include <future>
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/kafka_error_code.h>
#include "spinlock.h"

#pragma once

namespace csi
{
    namespace kafka
    {
        class basic_call_context
        {
        public:
            basic_call_context() : _tx_size(0), _rx_size(0), _rx_cursor(0), _expecting_reply(true){}

            enum { MAX_BUFFER_SIZE = 1024 * 1024 };
            typedef boost::function <void(const boost::system::error_code&, std::shared_ptr<basic_call_context>)>	callback;
            typedef std::shared_ptr<basic_call_context>					                                            handle;

            boost::array<uint8_t, MAX_BUFFER_SIZE>  _tx_buffer;
            size_t                                  _tx_size;
            boost::array<uint8_t, MAX_BUFFER_SIZE>  _rx_buffer;
            size_t                                  _rx_size;
            size_t                                  _rx_cursor;
            bool                                    _expecting_reply;
            callback                                _callback;
        };


        basic_call_context::handle create_metadata_request(const std::vector<std::string>& topics, int32_t correlation_id);
        basic_call_context::handle create_produce_request(const std::string& topic, int32_t partition_id, int required_acks, int timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id);
        basic_call_context::handle create_simple_offset_request(const std::string& topic, int32_t partition_id, int64_t time, int32_t max_number_of_offsets, int32_t correlation_id);
        basic_call_context::handle create_simple_fetch_request(const std::string& topic, int32_t partition_id, int64_t fetch_offset, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id);
        basic_call_context::handle create_multi_fetch_request(const std::string& topic, const std::vector<partition_cursor>&, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id);

        //CONSUMER OFFSET MANAGEMENT
        basic_call_context::handle create_consumer_metadata_request(const std::string& consumer_group, int32_t correlation_id);
        basic_call_context::handle create_simple_offset_commit_request(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int64_t offset, int64_t timestamp, const std::string& metadata, int32_t correlation_id);
        basic_call_context::handle create_simple_offset_fetch_request(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int32_t correlation_id);
        basic_call_context::handle create_simple_offset_fetch_request(const std::string& consumer_group, int32_t correlation_id);

        rpc_result<metadata_response>          parse_metadata_response(csi::kafka::basic_call_context::handle handle);
        rpc_result<produce_response>           parse_produce_response(csi::kafka::basic_call_context::handle handle);
        rpc_result<offset_response>            parse_offset_response(csi::kafka::basic_call_context::handle handle);
        rpc_result<fetch_response>             parse_fetch_response(csi::kafka::basic_call_context::handle handle);

        rpc_result<consumer_metadata_response> parse_consumer_metadata_response(csi::kafka::basic_call_context::handle handle);
        rpc_result<offset_commit_response>     parse_offset_commit_response(csi::kafka::basic_call_context::handle handle);
        rpc_result<offset_fetch_response>      parse_offset_fetch_response(csi::kafka::basic_call_context::handle handle);


        namespace low_level
        {
            class client
            {
            public:
                typedef boost::function < void(const boost::system::error_code&)>       completetion_handler;
                typedef boost::function <void(rpc_result<metadata_response>)>           get_metadata_callback;
                typedef boost::function <void(rpc_result<produce_response>)>            send_produce_callback;
                
                typedef boost::function <void(rpc_result<offset_response>)>             get_offset_callback;
                typedef boost::function <void(rpc_result<fetch_response>)>              get_data_callback;

                typedef boost::function <void(rpc_result<consumer_metadata_response>)>  get_consumer_metadata_callback;
                typedef boost::function <void(rpc_result<offset_commit_response>)>      commit_offset_callback;
                typedef boost::function <void(rpc_result<offset_fetch_response>)>       get_consumer_offset_callback;

                client(boost::asio::io_service& io_service);
                ~client();

                void                                           connect_async(const boost::asio::ip::tcp::resolver::query& query, completetion_handler handler);
                boost::system::error_code                      connect(const boost::asio::ip::tcp::resolver::query& query);

                bool                                           close();
                bool                                           is_connected() const;
                bool                                           is_connection_in_progress() const;

                void                                            get_metadata_async(const std::vector<std::string>& topics, int32_t correlation_id, get_metadata_callback);
                rpc_result<metadata_response>                   get_metadata(const std::vector<std::string>& topics, int32_t correlation_id);

                void                                            send_produce_async(const std::string& topic, int32_t partition_id, int required_acks, int timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, send_produce_callback);
                rpc_result<produce_response>                    send_produce(const std::string& topic, int32_t partition_id, int required_acks, int timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id);

                void                                            get_offset_async(const std::string& topic, int32_t partition, int64_t start_time, int32_t max_number_of_offsets, int32_t correlation_id, get_offset_callback);
                rpc_result<offset_response>                     get_offset(const std::string& topic, int32_t partition, int64_t start_time, int32_t max_number_of_offsets, int32_t correlation_id);

                void                                            get_data_async(const std::string& topic, const std::vector<partition_cursor>&, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id, get_data_callback);
                rpc_result<fetch_response>                      get_data(const std::string& topic, const std::vector<partition_cursor>&, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id);

                void                                            get_consumer_metadata_async(const std::string& consumer_group, int32_t correlation_id, get_consumer_metadata_callback);
                rpc_result<consumer_metadata_response>          get_consumer_metadata(const std::string& consumer_group, int32_t correlation_id);

                void                                            commit_consumer_offset_async(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int64_t offset, int64_t timestamp, const std::string& metadata, int32_t correlation_id, commit_offset_callback);
                rpc_result<offset_commit_response>              commit_consumer_offset(const std::string& consumer_group, const std::string& topic, int32_t partition, int64_t offset, int64_t timestamp, const std::string& metadata, int32_t correlation_id);

                void                                            get_consumer_offset_async(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int32_t correlation_id, get_consumer_offset_callback);
                rpc_result<offset_fetch_response>               get_consumer_offset(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int32_t correlation_id);

                void                                            get_consumer_offset_async(const std::string& consumer_group, int32_t correlation_id, get_consumer_offset_callback);
                rpc_result<offset_fetch_response>               get_consumer_offset(const std::string& consumer_group, int32_t correlation_id);


            protected:
                void                                            perform_async(basic_call_context::handle, basic_call_context::callback cb);
                csi::kafka::basic_call_context::handle          perform_sync(basic_call_context::handle, basic_call_context::callback cb);

                // asio callbacks
                void handle_timer(const boost::system::error_code& ec);

                void _perform(basic_call_context::handle handle);       // will be called in context of worker thread

                void handle_resolve(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);
                void handle_connect(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);

                void socket_rx_cb(const boost::system::error_code& e, size_t bytes_received, basic_call_context::handle handle);
                void socket_tx_cb(const boost::system::error_code& e, basic_call_context::handle handle);

                boost::asio::io_service&	              _io_service;
                csi::kafka::spinlock                      _spinlock;
                boost::asio::ip::tcp::resolver            _resolver;
                boost::asio::deadline_timer			      _timer;
                boost::posix_time::time_duration	      _timeout;
                boost::asio::ip::tcp::socket              _socket; // array of connections to shard leaders???
                std::deque<basic_call_context::handle>    _tx_queue;
                std::deque<basic_call_context::handle>    _rx_queue;
                bool                                      _connected;
                bool                                      _connection_in_progress;
                bool                                      _tx_in_progress;
                bool                                      _rx_in_progress;
            };
        }; // internal
    } // kafka
} // csi