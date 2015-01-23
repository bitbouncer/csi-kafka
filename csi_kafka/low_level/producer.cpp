#include "producer.h"
#include <boost/thread.hpp>
#include <boost/bind.hpp>

namespace csi
{
    namespace kafka
    {
        producer::producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition) :
            _ios(io_service),
            _client(io_service),
            _topic(topic),
            _partition_id(partition)
        {
        }

        void producer::connect_async(const broker_address& address, int32_t timeout, connect_callback cb)
        {
            _client.connect_async(address, timeout, cb);
        }

        boost::system::error_code producer::connect(const broker_address& address, int32_t timeout)
        {
            return _client.connect(address, timeout);
        }

        void producer::connect_async(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout, connect_callback cb)
        {
            _client.connect_async(query, timeout, cb);
        }

        boost::system::error_code producer::connect(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout)
        {
            return _client.connect(query, timeout);
        }

        void  producer::close()
        {
            _client.close();
        }

        void producer::send_async(int32_t required_acks, int32_t timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, send_callback cb)
        {
            _client.send_produce_async(_topic, _partition_id, required_acks, timeout, v, correlation_id, cb);
        }

        async_producer::async_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition, int32_t required_acks, int32_t timeout, int32_t max_packet_size) :
            _ios(io_service),
            _client(io_service),
            _topic(topic),
            _partition_id(partition),
            _required_acks(required_acks),
            _tx_timeout(timeout),
            _max_packet_size(max_packet_size),
            _tx_queue_byte_size(0),
            _tx_in_progress(false),
            _metrics_tx_kb_sec(boost::accumulators::tag::rolling_window::window_size = 50),
            _metrics_tx_msg_sec(boost::accumulators::tag::rolling_window::window_size = 50),
            _metrics_tx_roundtrip(boost::accumulators::tag::rolling_window::window_size = 10),
            _metrics_timer(io_service),
            _metrics_timeout(boost::posix_time::milliseconds(100)),
            _metrics_total_tx_kb(0),
            _metrics_total_tx_msg(0),
            __metrics_last_total_tx_kb(0),
            __metrics_last_total_tx_msg(0)
        {
            if (_max_packet_size <0)
                _max_packet_size = (csi::kafka::low_level::basic_call_context::MAX_BUFFER_SIZE - 1000);
            if (_max_packet_size >(csi::kafka::low_level::basic_call_context::MAX_BUFFER_SIZE - 1000))
                _max_packet_size = (csi::kafka::low_level::basic_call_context::MAX_BUFFER_SIZE - 1000);

            _metrics_timer.expires_from_now(_metrics_timeout);
            _metrics_timer.async_wait([this](const boost::system::error_code& ec){ handle_metrics_timer(ec); });
        }

        async_producer::~async_producer()
        {
            _metrics_timer.cancel();
            _client.close();
        }

        void  async_producer::close()
        {
            _client.close();
        }

        void async_producer::handle_metrics_timer(const boost::system::error_code& ec)
        {
            if (ec)
                return;

            uint64_t kb_sec = 10 * (_metrics_total_tx_kb - __metrics_last_total_tx_kb) / 1024;
            uint64_t msg_sec = 10 * (_metrics_total_tx_msg - __metrics_last_total_tx_msg);
            _metrics_tx_kb_sec((double) kb_sec);
            _metrics_tx_msg_sec((double) msg_sec);
            __metrics_last_total_tx_kb = _metrics_total_tx_kb;
            __metrics_last_total_tx_msg = _metrics_total_tx_msg;
            _metrics_timer.expires_from_now(_metrics_timeout);
            _metrics_timer.async_wait([this](const boost::system::error_code& ec){ handle_metrics_timer(ec); });
            
            _try_send();
        }

        void async_producer::connect_async(const broker_address& address, int32_t timeout, connect_callback cb)
        {
            _client.connect_async(address, timeout, cb);
        }

        boost::system::error_code async_producer::connect(const broker_address& address, int32_t timeout)
        {
            return _client.connect(address, timeout);
        }


        void async_producer::connect_async(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout, connect_callback cb)
        {
            _client.connect_async(query, 1000, cb);
        }

        boost::system::error_code async_producer::connect(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout)
        {
            return _client.connect(query, 1000);
        }

        void async_producer::send_async(std::shared_ptr<basic_message> message, tx_ack_callback cb)
        {
            {
                csi::kafka::spinlock::scoped_lock xxx(_spinlock);
                _tx_queue_byte_size += message ? message->size() : 0; // callback markers
                _tx_queue.push_front(tx_item(message, cb));
            }

            if (_tx_in_progress)
                return;

            _ios.post([this](){_try_send(); });
        }

     /*   void  async_producer::wait_lowwater_async(int max_messages, lowwater_callback)
        {
            
        }*/


        void async_producer::_try_send()
        {
            if (_tx_in_progress || !_client.is_connected())
                return;

            //std::cerr << "+";
            _tx_in_progress = true;

            std::vector<std::shared_ptr<basic_message>> v;

            // we might have NULL data with callbacks in this stream - it's a callback marker after the last inserted batch 
            // it should be included in the nr of items sent so we can remove it after completion
            size_t items_in_batch = 0;
            {
                csi::kafka::spinlock::scoped_lock xxx(_spinlock);
                size_t remaining = _max_packet_size;
                {
                    std::deque<tx_item> ::reverse_iterator cursor = _tx_queue.rbegin();
                    while (cursor != _tx_queue.rend())
                    {
                        size_t item_size = (*cursor).msg ? (*cursor).msg->size() : 0;
                        if (remaining < item_size)
                            break;
                        ++items_in_batch;
                        remaining -= item_size;
                        if ((*cursor).msg)
                            v.push_back((*cursor).msg);
                        cursor++;
                    }
                    assert(_tx_queue.size() >= v.size());
                }
            }
            

            if (items_in_batch > 0)
            {
                auto tick = boost::posix_time::microsec_clock::local_time();
                _client.send_produce_async(_topic, _partition_id, _required_acks, _tx_timeout, v, 99, [this, tick, items_in_batch](rpc_result<produce_response> result)
                {
                    auto now = boost::posix_time::microsec_clock::local_time();
                    boost::posix_time::time_duration diff = now - tick;
                    _metrics_tx_roundtrip((double) diff.total_milliseconds());

                    //TODO PARSE THE RESULT DEEP - WE MIGHT HAVE GOTTEN AN ERROR INSIDE
                    //IF SO WE SHOULD PROBASBLY CLOSE THE CONNECTION
                    if (result)
                    {
                        std::cerr << " _client.send_produce_async failed " << csi::kafka::to_string(result.ec) << std::endl;
                        _tx_in_progress = false;
                        _client.close();
                        //_try_send();
                        return;
                    }

                    std::vector<tx_ack_callback> callbacks; // we cant run the callbacks when the spilock is locked so copy those and run them after
                    {
                        csi::kafka::spinlock::scoped_lock xxx(_spinlock);
                        {
                            assert(_tx_queue.size() >= items_in_batch);
                            size_t tx_size = 0;
                            for (size_t i = 0; i != items_in_batch; ++i)
                            {
                                const tx_item& item = *(_tx_queue.end() - 1);
                                if (item.msg) // we might have a callback marker (NULL message)
                                {
                                    tx_size += item.msg->size();
                                    ++_metrics_total_tx_msg;
                                }
                                if (item.cb)
                                    callbacks.push_back(item.cb);
                                _tx_queue.pop_back();
                            }
                            _tx_queue_byte_size -= tx_size;
                            _metrics_total_tx_kb += tx_size;
                        }
                    }
                    for (std::vector<tx_ack_callback>::const_iterator i = callbacks.begin(); i != callbacks.end(); ++i)
                        (*i)();

                    _tx_in_progress = false; // here is a small gap where an external enqueu would trigger a post(_try_send()) - thus callinmg twice but it should be harmless
                    _try_send();
                });
            }
            else
            {
                _tx_in_progress = false; // here is a small gap where an external enqueu would trigger a post(_try_send()) - thus callinmg twice but it should be harmless
            }
        }
    } // kafka
}; // csi
