#include <deque>
#include <future>
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <csi_kafka/kafka.h>
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
            typedef boost::function <void(csi::kafka::error_codes, std::shared_ptr<basic_call_context>)>	callback;
            typedef std::shared_ptr<basic_call_context>					                                    handle;

            boost::array<uint8_t, MAX_BUFFER_SIZE>  _tx_buffer;
            size_t                                  _tx_size;
            boost::array<uint8_t, MAX_BUFFER_SIZE>  _rx_buffer;
            size_t                                  _rx_size;
            size_t                                  _rx_cursor;
            bool                                    _expecting_reply;
            callback                                _callback;
        };

        basic_call_context::handle create_produce_request(const std::string& topic, int32_t partition_id, int required_acks, int timeout, const std::vector<basic_message>& v, int32_t correlation_id);
        basic_call_context::handle create_metadata_request(const std::vector<std::string>& topics, int32_t correlation_id);
        basic_call_context::handle create_simple_fetch_request(const std::string& topic, int32_t partition_id, uint32_t max_wait_time, size_t min_bytes, int64_t fetch_offset, int32_t correlation_id);
        basic_call_context::handle create_simple_offset_request(const std::string& topic, int32_t partition_id, int64_t time, int32_t max_number_of_offsets, int32_t correlation_id);
        basic_call_context::handle create_consumer_metadata_request(const std::string& consumer_group, int32_t correlation_id);
        basic_call_context::handle create_simple_offset_commit_request(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int64_t offset, int64_t timestamp, const std::string& metadata, int32_t correlation_id);
        basic_call_context::handle create_simple_offset_fetch_request(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int32_t correlation_id);

        std::shared_ptr<produce_response>           parse_produce_response(csi::kafka::basic_call_context::handle handle);
        std::shared_ptr<fetch_response>             parse_fetch_response(csi::kafka::basic_call_context::handle handle);
        std::shared_ptr<offset_response>            parse_offset_response(csi::kafka::basic_call_context::handle handle);
        std::shared_ptr<metadata_response>          parse_metadata_response(csi::kafka::basic_call_context::handle handle);
        std::shared_ptr<offset_commit_response>     parse_offset_commit_response(csi::kafka::basic_call_context::handle handle);
        std::shared_ptr<offset_fetch_response>      parse_offset_fetch_response(csi::kafka::basic_call_context::handle handle);
        std::shared_ptr<consumer_metadata_response> parse_consumer_metadata_response(csi::kafka::basic_call_context::handle handle);

        namespace low_level
        {
            class client
            {
            public:
                typedef boost::function < void(const boost::system::error_code&)>                           completetion_handler;
                typedef boost::function <void(csi::kafka::error_codes, std::shared_ptr<metadata_response>)> get_metadata_callback;

                client(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query);
                ~client();


                void connect_async(completetion_handler handler);
                boost::system::error_code connect();

                bool close();
                bool is_connected() const;

                void                                get_metadata_async(const std::vector<std::string>& topics, int32_t correlation_id, get_metadata_callback);
                std::shared_ptr<metadata_response>  get_metadata(const std::vector<std::string>& topics, int32_t correlation_id);
                
                void                                perform_async(basic_call_context::handle, basic_call_context::callback cb);
                basic_call_context::handle          perform_sync(basic_call_context::handle, basic_call_context::callback cb);

            protected:
                //void tryconnect();

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
                boost::asio::ip::tcp::resolver::query     _query;
                boost::asio::deadline_timer			      _timer;
                boost::posix_time::time_duration	      _timeout;
                boost::asio::ip::tcp::socket              _socket; // array of connections to shard leaders???
                std::deque<basic_call_context::handle>    _tx_queue;
                std::deque<basic_call_context::handle>    _rx_queue;

                bool                                      _connecting;
                bool                                      _connected;
                bool                                      _tx_in_progress;
                bool                                      _rx_in_progress;
            };
        }; // internal
    } // kafka
} // csi