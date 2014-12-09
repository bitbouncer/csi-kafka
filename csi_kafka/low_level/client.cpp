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

        basic_call_context::handle create_simple_fetch_request(const std::string& topic, int32_t partition_id, int64_t fetch_offset, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id)
        {
            basic_call_context::handle handle(new basic_call_context());
            handle->_tx_size = encode_simple_fetch_request(topic, partition_id, fetch_offset, max_wait_time, min_bytes, correlation_id, (char*)&handle->_tx_buffer[0], basic_call_context::MAX_BUFFER_SIZE);
            return handle;
        }

        basic_call_context::handle create_multi_fetch_request(const std::string& topic, const std::vector<partition_cursor>& cursors, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id)
        {
            basic_call_context::handle handle(new basic_call_context());
            handle->_tx_size = encode_multi_fetch_request(topic, cursors, max_wait_time, min_bytes, correlation_id, (char*)&handle->_tx_buffer[0], basic_call_context::MAX_BUFFER_SIZE);
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
            client::client(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query) :
                _io_service(io_service),
                _resolver(io_service),
                _query(query),
                _timer(io_service),
                _socket(io_service),
                _connecting(false),
                _connected(false),
                _tx_in_progress(false),
                _rx_in_progress(false),
                _timeout(boost::posix_time::milliseconds(1000))
            {
                _timer.expires_from_now(_timeout);
                _timer.async_wait(boost::bind(&client::handle_timer, this, boost::asio::placeholders::error));
            }

            client::~client()
            {
                _timer.cancel();
                close();
            }

            void client::connect_async(completetion_handler cb)
            {
                boost::asio::ip::tcp::resolver::iterator iterator = _resolver.resolve(_query);
                boost::asio::async_connect(_socket, iterator, [this, cb](boost::system::error_code ec, boost::asio::ip::tcp::resolver::iterator)
                {
                    if (!ec)
                        _connected = true;
                    cb(ec);
                });
            }

            boost::system::error_code client::connect()
            {
                std::promise<boost::system::error_code> p;
                std::future<boost::system::error_code>  f = p.get_future();
                connect_async([&p](const boost::system::error_code& error)
                {
                    p.set_value(error);
                });
                f.wait();
                return f.get();
            }

            void client::handle_timer(const boost::system::error_code& ec)
            {
                if (!ec)
                {
                    _timer.expires_from_now(_timeout);
                    _timer.async_wait(boost::bind(&client::handle_timer, this, boost::asio::placeholders::error));
                }
            }
                
            bool client::close()
            {
                //if (_connecting) { return false; }
                _connected = false;
                boost::system::error_code ec;
                _socket.cancel(ec);
                _socket.close();
                return true;
            }

            bool client::is_connected() const
            {
                return _connected;
            }

            void client::get_metadata_async(const std::vector<std::string>& topics, int32_t correlation_id, get_metadata_callback cb)
            {
                perform_async(csi::kafka::create_metadata_request(topics, correlation_id), [cb](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
                {
                    if (!ec)
                    {
                        auto response = csi::kafka::parse_metadata_response(handle);
                        cb(ec, csi::kafka::NoError, response);
                    }
                    else
                    {
                        cb(ec, csi::kafka::NoError, std::shared_ptr<metadata_response>(NULL));
                    }
                });
            }

            std::shared_ptr<metadata_response> client::get_metadata(const std::vector<std::string>& topics, int32_t correlation_id)
            {
                std::promise<std::shared_ptr<metadata_response>> p;
                std::future<std::shared_ptr<metadata_response>>  f = p.get_future();
                get_metadata_async(topics, correlation_id, [&p](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, std::shared_ptr<metadata_response> response)
                {
                    p.set_value(response);
                });
                f.wait();
                return f.get();
            }

            void client::perform_async(basic_call_context::handle handle, basic_call_context::callback cb)
            {
                handle->_callback = cb;
                _io_service.post(boost::bind(&client::_perform, this, handle));
            }

            //we SHOULD remove callback here and reurn a pair <ec, handle>
            basic_call_context::handle client::perform_sync(basic_call_context::handle handle, basic_call_context::callback cb)
            {
                std::promise<boost::system::error_code> promise;
                std::future<boost::system::error_code> future = promise.get_future();
                perform_async(handle, [&promise](const boost::system::error_code& ec1, std::shared_ptr<basic_call_context>)
                {
                    promise.set_value(ec1);
                });
                future.wait();
                auto res = future.get();
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
                if (error_code)
                {
                    if (handle->_callback)
                        handle->_callback(error_code, handle);
                }

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
                        handle->_callback(error_code, handle);

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
                    if (handle->_callback)
                        handle->_callback(error_code, handle);
                    return;
                }

                // if we're not expecing result the all we can say to the client is "NoError" when we posted the data on socket
                if (!handle->_expecting_reply)
                    handle->_callback(error_code, handle);

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