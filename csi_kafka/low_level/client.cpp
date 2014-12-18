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


        namespace low_level
        {
            basic_call_context::handle create_produce_request(const std::string& topic, int partition, int required_acks, int timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id)
            {
                basic_call_context::handle handle(new basic_call_context());
                handle->_expecting_reply = (required_acks != 0);
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

            basic_call_context::handle create_simple_offset_fetch_request(const std::string& consumer_group, int32_t correlation_id)
            {
                basic_call_context::handle handle(new basic_call_context());
                handle->_tx_size = encode_offset_fetch_all_request(consumer_group, correlation_id, (char*)&handle->_tx_buffer[0], basic_call_context::MAX_BUFFER_SIZE);
                return handle;
            }


            static rpc_result<produce_response>           parse_produce_response(basic_call_context::handle handle)           { return csi::kafka::parse_produce_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
            static rpc_result<fetch_response>             parse_fetch_response(basic_call_context::handle handle)             { return csi::kafka::parse_fetch_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
            static rpc_result<offset_response>            parse_offset_response(basic_call_context::handle handle)            { return csi::kafka::parse_offset_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
            static rpc_result<metadata_response>          parse_metadata_response(basic_call_context::handle handle)          { return csi::kafka::parse_metadata_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
            static rpc_result<offset_commit_response>     parse_offset_commit_response(basic_call_context::handle handle)     { return csi::kafka::parse_offset_commit_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
            static rpc_result<offset_fetch_response>      parse_offset_fetch_response(basic_call_context::handle handle)      { return csi::kafka::parse_offset_fetch_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }
            static rpc_result<consumer_metadata_response> parse_consumer_metadata_response(basic_call_context::handle handle) { return csi::kafka::parse_consumer_metadata_response((const char*)&handle->_rx_buffer[0], handle->_rx_size); }



            client::client(boost::asio::io_service& io_service) :
                _io_service(io_service),
                _resolver(io_service),
                _timer(io_service),
                _socket(io_service),
                _connected(false),
                _connection_in_progress(false),
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

            void client::connect_async(const boost::asio::ip::tcp::resolver::query& query, completetion_handler cb)
            {
                _connection_in_progress = true;
                _resolver.async_resolve(query, [this, cb](boost::system::error_code ec, boost::asio::ip::tcp::resolver::iterator iterator)
                {
                    if (ec)
                    {
                        _connection_in_progress = false;
                        cb(ec);
                    }
                    else
                        boost::asio::async_connect(_socket, iterator, [this, cb](boost::system::error_code ec, boost::asio::ip::tcp::resolver::iterator)
                    {
                        _connection_in_progress = false;
                        if (!ec)
                            _connected = true;
                        cb(ec);
                    });
                });
            }

            boost::system::error_code client::connect(const boost::asio::ip::tcp::resolver::query& query)
            {
                std::promise<boost::system::error_code> p;
                std::future<boost::system::error_code>  f = p.get_future();
                connect_async(query, [&p](const boost::system::error_code& error)
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

            bool client::is_connection_in_progress() const
            {
                return _connection_in_progress;
            }

            void client::get_metadata_async(const std::vector<std::string>& topics, int32_t correlation_id, get_metadata_callback cb)
            {
                perform_async(create_metadata_request(topics, correlation_id), [cb](const boost::system::error_code& ec, basic_call_context::handle handle)
                {
                    if (ec)
                        cb(rpc_result<metadata_response>(ec));
                    else
                        cb(parse_metadata_response(handle));
                });
            }

            rpc_result<metadata_response> client::get_metadata(const std::vector<std::string>& topics, int32_t correlation_id)
            {
                std::promise<rpc_result<metadata_response>> p;
                std::future<rpc_result<metadata_response>>  f = p.get_future();
                get_metadata_async(topics, correlation_id, [&p](rpc_result<metadata_response> response)
                {
                    p.set_value(response);
                });
                f.wait();
                return f.get();
            }

            void client::send_produce_async(const std::string& topic, int32_t partition_id, int required_acks, int timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, send_produce_callback cb)
            {
                perform_async(create_produce_request(topic, partition_id, required_acks, timeout, v, correlation_id), [cb](const boost::system::error_code& ec, basic_call_context::handle handle)
                {
                    if (ec)
                        cb(rpc_result<produce_response>(ec));
                    else
                        cb(parse_produce_response(handle));
                });
            }
            
            rpc_result<produce_response> client::send_produce(const std::string& topic, int32_t partition_id, int required_acks, int timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id)
            {
                std::promise<rpc_result<produce_response>> p;
                std::future<rpc_result<produce_response>>  f = p.get_future();
                send_produce_async(topic, partition_id, required_acks, timeout, v, correlation_id, [&p](rpc_result<produce_response> response)
                {
                    p.set_value(response);
                });
                f.wait();
                return f.get();
            }

            void client::get_data_async(const std::string& topic, const std::vector<partition_cursor>& partitions, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id, get_data_callback cb)
            {
                perform_async(create_multi_fetch_request(topic, partitions, max_wait_time, min_bytes, correlation_id), [cb](const boost::system::error_code& ec, basic_call_context::handle handle)
                {
                    if (ec)
                        cb(rpc_result<fetch_response>(ec));
                    else
                        cb(parse_fetch_response(handle));
                });
            }

            rpc_result<fetch_response> client::get_data(const std::string& topic, const std::vector<partition_cursor>& partitions, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id)
            {
                std::promise<rpc_result<fetch_response>> p;
                std::future<rpc_result<fetch_response>>  f = p.get_future();
                get_data_async(topic, partitions, max_wait_time, min_bytes, correlation_id, [&p](rpc_result<fetch_response> response)
                {
                    p.set_value(response);
                });
                f.wait();
                return f.get();
            }



            void client::get_consumer_metadata_async(const std::string& consumer_group, int32_t correlation_id, get_consumer_metadata_callback cb)
            {
                perform_async(create_consumer_metadata_request(consumer_group, correlation_id), [cb](const boost::system::error_code& ec, basic_call_context::handle handle)
                {
                    if (ec)
                        cb(rpc_result<consumer_metadata_response>(ec));
                    else
                        cb(parse_consumer_metadata_response(handle));
                });
            }

            rpc_result<consumer_metadata_response>  client::get_consumer_metadata(const std::string& consumer_group, int32_t correlation_id)
            {
                std::promise<rpc_result<consumer_metadata_response> > p;
                std::future<rpc_result<consumer_metadata_response>>  f = p.get_future();
                get_consumer_metadata_async(consumer_group, correlation_id, [&p](rpc_result<consumer_metadata_response> response)
                {
                    p.set_value(response);
                });
                f.wait();
                return f.get();
            }

            void client::get_offset_async(const std::string& topic, int32_t partition, int64_t start_time, int32_t max_number_of_offsets, int32_t correlation_id, get_offset_callback cb)
            {
                perform_async(create_simple_offset_request(topic, partition, start_time, max_number_of_offsets, correlation_id), [this, cb](const boost::system::error_code& ec, basic_call_context::handle handle)
                {
                    if (ec)
                        cb(rpc_result<offset_response>(ec));
                    else
                        cb(parse_offset_response(handle));
                });
            }

            rpc_result<offset_response> client::get_offset(const std::string& topic, int32_t partition, int64_t start_time, int32_t max_number_of_offsets, int32_t correlation_id)
            {
                std::promise<rpc_result<offset_response> > p;
                std::future<rpc_result<offset_response>>  f = p.get_future();
                get_offset_async(topic, partition, start_time, max_number_of_offsets, correlation_id, [&p](rpc_result<offset_response> response)
                {
                    p.set_value(response);
                });
                f.wait();
                return f.get();
            }


            void client::commit_consumer_offset_async(const std::string& consumer_group, const std::string& topic, int32_t partition, int64_t offset, int64_t timestamp, const std::string& metadata, int32_t correlation_id, commit_offset_callback cb)
            {
                perform_async(create_simple_offset_commit_request(consumer_group, topic, partition, offset, timestamp, metadata, correlation_id), [this, cb](const boost::system::error_code& ec, basic_call_context::handle handle)
                {
                    if (ec)
                        cb(rpc_result<offset_commit_response>(ec));
                    else
                        cb(parse_offset_commit_response(handle));
                });
            }

            rpc_result<offset_commit_response> client::commit_consumer_offset(const std::string& consumer_group, const std::string& topic, int32_t partition, int64_t offset, int64_t timestamp, const std::string& metadata, int32_t correlation_id)
            {
                std::promise<rpc_result<offset_commit_response> > p;
                std::future<rpc_result<offset_commit_response>>  f = p.get_future();
                commit_consumer_offset_async(consumer_group, topic, partition, offset, timestamp, metadata, correlation_id, [&p](rpc_result<offset_commit_response> response)
                {
                    p.set_value(response);
                });
                f.wait();
                return f.get();
            }

            void client::get_consumer_offset_async(const std::string& consumer_group, const std::string& topic, int32_t partition, int32_t correlation_id, get_consumer_offset_callback cb)
            {
                perform_async(create_simple_offset_fetch_request(consumer_group, topic, partition, correlation_id), [this, cb](const boost::system::error_code& ec, basic_call_context::handle handle)
                {
                    if (ec)
                        cb(rpc_result<offset_fetch_response>(ec));
                    else
                        cb(parse_offset_fetch_response(handle));
                });
            }

            rpc_result<offset_fetch_response> client::get_consumer_offset(const std::string& consumer_group, const std::string& topic, int32_t partition, int32_t correlation_id)
            {
                std::promise<rpc_result<offset_fetch_response> > p;
                std::future<rpc_result<offset_fetch_response>>  f = p.get_future();
                get_consumer_offset_async(consumer_group, topic, partition, correlation_id, [&p](rpc_result<offset_fetch_response> response)
                {
                    p.set_value(response);
                });
                f.wait();
                return f.get();
            }



            void client::get_consumer_offset_async(const std::string& consumer_group, int32_t correlation_id, get_consumer_offset_callback cb)
            {
                perform_async(create_simple_offset_fetch_request(consumer_group, correlation_id), [this, cb](const boost::system::error_code& ec, basic_call_context::handle handle)
                {
                    if (ec)
                        cb(rpc_result<offset_fetch_response>(ec));
                    else
                        cb(parse_offset_fetch_response(handle));
                });
            }

            rpc_result<offset_fetch_response> client::get_consumer_offset(const std::string& consumer_group, int32_t correlation_id)
            {
                std::promise<rpc_result<offset_fetch_response> > p;
                std::future<rpc_result<offset_fetch_response>>  f = p.get_future();
                get_consumer_offset_async(consumer_group, correlation_id, [&p](rpc_result<offset_fetch_response> response)
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