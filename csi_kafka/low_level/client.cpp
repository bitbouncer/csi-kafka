#include <future>
#include <boost/lexical_cast.hpp>
#include <csi_kafka/kafka.h>
#include "client.h"
#include "decoder.h"
#include "encoder.h"

namespace csi
{
    namespace kafka
    {
        basic_call_context::handle create_produce_request(const std::string& topic, int partition, int required_acks, int timeout, const std::vector<basic_message>& v, int32_t correlation_id)
        {
            basic_call_context::handle handle(new basic_call_context());
            handle->_expecting_reply = (required_acks > 0);
            handle->_tx_size = encode_produce_request(topic, partition, required_acks, timeout, v, correlation_id, (char*)&handle->_tx_buffer[0], basic_call_context::MAX_BUFFER_SIZE);
            return handle;
        }

        basic_call_context::handle create_metadata_request(const std::vector<std::string>& topics, int32_t correlation_id)
        {
            basic_call_context::handle handle(new basic_call_context());
            handle->_tx_size = encode_metadata_request(topics, correlation_id, (char*)&handle->_tx_buffer[0], basic_call_context::MAX_BUFFER_SIZE);
            return handle;
        }

        basic_call_context::handle create_simple_fetch_request(const std::string& topic, int32_t partition_id, uint32_t max_wait_time, size_t min_bytes, int64_t fetch_offset, int32_t correlation_id)
        {
            basic_call_context::handle handle(new basic_call_context());
            handle->_tx_size = encode_simple_fetch_request(topic, partition_id, max_wait_time, min_bytes, fetch_offset, correlation_id, (char*)&handle->_tx_buffer[0], basic_call_context::MAX_BUFFER_SIZE);
            return handle;
        }

        basic_call_context::handle create_simple_offset_request(const std::string& topic, int32_t partition_id, int64_t time, int32_t max_number_of_offsets, int32_t correlation_id)
        {
            basic_call_context::handle handle(new basic_call_context());
            handle->_tx_size = encode_simple_offset_request(topic, partition_id, time, max_number_of_offsets, correlation_id, (char*)&handle->_tx_buffer[0], basic_call_context::MAX_BUFFER_SIZE);
            return handle;
        }

        basic_call_context::handle create_consumer_metadata_request(const std::string& consumer_group, int32_t correlation_id)
        {
            basic_call_context::handle handle(new basic_call_context());
            handle->_tx_size = encode_consumer_metadata_request(consumer_group, correlation_id, (char*)&handle->_tx_buffer[0], basic_call_context::MAX_BUFFER_SIZE);
            return handle;
        }

        basic_call_context::handle create_simple_offset_commit_request(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int64_t offset, int64_t timestamp, const std::string& metadata, int32_t correlation_id)
        {
            basic_call_context::handle handle(new basic_call_context());
            handle->_tx_size = encode_simple_offset_commit_request(consumer_group, topic, partition_id, offset, timestamp, metadata, correlation_id, (char*)&handle->_tx_buffer[0], basic_call_context::MAX_BUFFER_SIZE);
            return handle;
        }

        basic_call_context::handle create_simple_offset_fetch_request(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int32_t correlation_id)
        {
            basic_call_context::handle handle(new basic_call_context());
            handle->_tx_size = encode_simple_offset_fetch_request(consumer_group, topic, partition_id, correlation_id, (char*)&handle->_tx_buffer[0], basic_call_context::MAX_BUFFER_SIZE);
            return handle;
        }

        std::shared_ptr<produce_response>           parse_produce_response(csi::kafka::basic_call_context::handle handle)           { return parse_produce_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
        std::shared_ptr<fetch_response>             parse_fetch_response(csi::kafka::basic_call_context::handle handle)             { return parse_fetch_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
        std::shared_ptr<offset_response>            parse_offset_response(csi::kafka::basic_call_context::handle handle)            { return parse_offset_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
        std::shared_ptr<metadata_response>          parse_metadata_response(csi::kafka::basic_call_context::handle handle)          { return parse_metadata_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
        std::shared_ptr<offset_commit_response>     parse_offset_commit_response(csi::kafka::basic_call_context::handle handle)     { return parse_offset_commit_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
        std::shared_ptr<offset_fetch_response>      parse_offset_fetch_response(csi::kafka::basic_call_context::handle handle)      { return parse_offset_fetch_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
        std::shared_ptr<consumer_metadata_response> parse_consumer_metadata_response(csi::kafka::basic_call_context::handle handle) { return parse_consumer_metadata_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }

        namespace low_level
        {
            client::client(boost::asio::io_service& io_service) :
                _io_service(io_service),
                _resolver(io_service),
                _socket(io_service),
                _connecting(false),
                _connected(false),
                _tx_in_progress(false),
                _rx_in_progress(false)
            {
            }

            client::~client()
            {
                close();
            }

            bool client::connect(const std::string& hostname, const uint16_t port)
            {
                return connect(hostname, boost::lexical_cast<std::string>(port));
            }

            bool client::connect(const std::string& hostname, const std::string& servicename)
            {
                if (_connecting) { return false; }
                _connecting = true;

                boost::asio::ip::tcp::resolver::query query(hostname, servicename);
                _resolver.async_resolve(query, boost::bind(&client::handle_resolve, this, boost::asio::placeholders::error, boost::asio::placeholders::iterator));
                return true;
            }

            bool client::close()
            {
                if (_connecting) { return false; }

                _connected = false;
                _socket.close();
                return true;
            }

            bool client::is_connected() const
            {
                return _connected;
            }

            bool client::is_connecting() const
            {
                return _connecting;
            }

            void client::handle_resolve(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints)
            {
                if (!error_code)
                {
                    boost::asio::ip::tcp::endpoint endpoint = *endpoints;
                    _socket.async_connect(endpoint, boost::bind(&client::handle_connect, this, boost::asio::placeholders::error, ++endpoints));
                }
                else
                {
                    _connecting = false;
                    //fail_fast_error_handler(error_code);
                }
            }

            void client::handle_connect(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints)
            {
                if (!error_code)
                {
                    // The connection was successful.
                    _connecting = false;
                    _connected = true;
                }
                else if (endpoints != boost::asio::ip::tcp::resolver::iterator())
                {
                    // TODO: handle connection error (we might not need this as we have others though?)

                    // The connection failed, but we have more potential endpoints so throw it back to handle resolve
                    _socket.close();
                    handle_resolve(boost::system::error_code(), endpoints);
                }
                else
                {
                    _connecting = false;
                    //fail_fast_error_handler(error_code);
                }
            }

            void client::perform_async(basic_call_context::handle handle, basic_call_context::callback cb)
            {
                handle->_callback = cb;
                _io_service.post(boost::bind(&client::_perform, this, handle));
            }

            static void wait_for_completion(csi::kafka::error_codes ec, std::promise<csi::kafka::error_codes>* promise)
            {
                promise->set_value(ec);
            }

            basic_call_context::handle client::perform_sync(basic_call_context::handle handle, basic_call_context::callback cb)
            {
                std::promise<csi::kafka::error_codes> promise; // just a dummy value - we use the request handle anyway as result
                std::future<csi::kafka::error_codes> future = promise.get_future();
                perform_async(handle, boost::bind(wait_for_completion, _1, &promise));
                future.wait();
                csi::kafka::error_codes res = future.get();
                if (cb)
                    cb(res, handle);
                return handle;
            }

            void client::_perform(basic_call_context::handle handle)
            {
                basic_call_context::handle item_to_send;
                basic_call_context::handle item_to_receive;

                {
                    spinlock::scoped_lock xx(_spinlock);
                    _tx_queue.push_back(handle);
                    if (handle->_expecting_reply)
                        _rx_queue.push_back(handle);

                    if (!_tx_in_progress)
                    {
                        item_to_send = _tx_queue[0];
                        _tx_queue.pop_front();
                        _tx_in_progress = true;
                    }

                    if (!_rx_in_progress)
                    {
                        if (_rx_queue.size())
                        {
                            // start a new read - begin with only the size data ... 4 bytes..
                            item_to_receive = _rx_queue[0];
                            _rx_queue.pop_front();
                            _rx_in_progress = true;
                        }
                    }
                }

                if (item_to_send)
                    boost::asio::async_write(_socket, boost::asio::buffer(item_to_send->_tx_buffer, item_to_send->_tx_size), boost::bind(&client::socket_tx_cb, this, boost::asio::placeholders::error, item_to_send));

                if (item_to_receive)
                    boost::asio::async_read(_socket, boost::asio::buffer(&item_to_receive->_rx_buffer, 4), boost::bind(&client::socket_rx_cb, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, item_to_receive));
            }

            void client::socket_rx_cb(const boost::system::error_code& error_code, size_t bytes_received, basic_call_context::handle handle)
            {
                handle->_rx_cursor += bytes_received;
                // a bit ugly but simple
                if (handle->_rx_cursor == 4)
                {
                    handle->_rx_size = ntohl(*(u_long*)&handle->_rx_buffer[0]);
                    // restart read at offset 0
                    handle->_rx_cursor = 0;
                    boost::asio::async_read(_socket, boost::asio::buffer(&handle->_rx_buffer[0], handle->_rx_size), boost::bind(&client::socket_rx_cb, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, handle));
                }
                else
                {
                    if (handle->_callback)
                        handle->_callback(csi::kafka::NoError, handle);

                    // start the next read
                    basic_call_context::handle item_to_receive;
                    {
                        spinlock::scoped_lock xx(_spinlock);
                        if (_rx_queue.size())
                        {
                            item_to_receive = _rx_queue[0];
                            _rx_queue.pop_front();
                        }
                    }

                    if (item_to_receive)
                        boost::asio::async_read(_socket, boost::asio::buffer(&item_to_receive->_rx_buffer, 4), boost::bind(&client::socket_rx_cb, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, item_to_receive));
                    else
                        _rx_in_progress = false;
                }
            }

            void client::socket_tx_cb(const boost::system::error_code& error_code, basic_call_context::handle handle)
            {
                if (error_code)
                {
                    // disconnected - do something else
                    // as opposed to canceled
                    //fail_fast_error_handler(error_code);
                }

                // if we're not expecing result the all we can say to the client is "NoError" when we posted the data on socket
                if (!handle->_expecting_reply)
                    handle->_callback(csi::kafka::NoError, handle);

                //more to send
                basic_call_context::handle item_to_send;
                {
                    spinlock::scoped_lock xx(_spinlock);
                    if (_tx_queue.size())
                    {
                        item_to_send = _tx_queue[0];
                        _tx_queue.pop_front();
                    }
                }

                if (item_to_send)
                    boost::asio::async_write(_socket, boost::asio::buffer(item_to_send->_tx_buffer, item_to_send->_tx_size), boost::bind(&client::socket_tx_cb, this, boost::asio::placeholders::error, item_to_send));
                else
                    _tx_in_progress = false;
            }
        }; // internal
    } // kafka
} // csi