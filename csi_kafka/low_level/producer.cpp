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
            _topic_name(topic),
            _partition_id(partition)
        {
        }

        void producer::connect_async(const boost::asio::ip::tcp::resolver::query& query, connect_callback cb)
        {
            _client.connect_async(query, cb);
        }

        boost::system::error_code producer::connect(const boost::asio::ip::tcp::resolver::query& query)
        {
            return _client.connect(query);
        }

        void  producer::close()
        {
            _client.close();
        }

        void producer::send_async(int32_t required_acks, int32_t timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, send_callback cb)
        {
            _client.send_produce_async(_topic_name, _partition_id, required_acks, timeout, v, correlation_id, cb);
        }

        async_producer::async_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition) :
            _ios(io_service),
            _client(io_service),
            _topic_name(topic),
            _partition_id(partition),
            _tx_queue_byte_size(0),
            _tx_in_progress(false),
            _required_acks(-1),
            _tx_timeout(500),
            _metrics_tx_kb_sec(boost::accumulators::tag::rolling_window::window_size = 50),
            _metrics_tx_msg_sec(boost::accumulators::tag::rolling_window::window_size = 50),
            _metrics_timer(io_service),
            _metrics_timeout(boost::posix_time::milliseconds(100)),
            _metrics_total_tx_kb(0),
            _metrics_total_tx_msg(0),
            __metrics_last_total_tx_kb(0),
            __metrics_last_total_tx_msg(0)
        {
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
        }

        void async_producer::connect_async(const boost::asio::ip::tcp::resolver::query& query, connect_callback cb)
        {
            _client.connect_async(query, cb);
        }

        boost::system::error_code async_producer::connect(const boost::asio::ip::tcp::resolver::query& query)
        {
            return _client.connect(query);
        }


        void async_producer::enqueue(std::shared_ptr<basic_message> message)
        {
            {
                csi::kafka::spinlock::scoped_lock xxx(_spinlock);
                _tx_queue_byte_size += message->size();
                _tx_queue.push_front(message);
            }

            if (_tx_in_progress)
                return;

            _ios.post([this](){_try_send(); });



            csi::kafka::spinlock::scoped_lock xxx(_spinlock);
        }

     /*   void  async_producer::wait_lowwater_async(int max_messages, lowwater_callback)
        {
            
        }*/


        void async_producer::_try_send()
        {
            if (_tx_in_progress || !_client.is_connected())
                return;

            _tx_in_progress = true;

            std::vector<std::shared_ptr<basic_message>> v;

            {
                csi::kafka::spinlock::scoped_lock xxx(_spinlock);
                size_t remaining = csi::kafka::basic_call_context::MAX_BUFFER_SIZE - 1000;
                {
                    std::deque<std::shared_ptr<basic_message>>::reverse_iterator cursor = _tx_queue.rbegin();
                    while (cursor != _tx_queue.rend())
                    {
                        size_t item_size = (*cursor)->size();
                        if (remaining < item_size)
                            break;

                        remaining -= item_size;
                        v.push_back(*cursor);
                        cursor++;
                    }
                    assert(_tx_queue.size() >= v.size());
                }
            }
            size_t items_in_batch = v.size();

            if (items_in_batch > 0)
            {
                _client.send_produce_async(_topic_name, _partition_id, _required_acks, _tx_timeout, v, 99, [this, items_in_batch](rpc_result<produce_response> result)
                {
                    if (result)
                    {
                        std::cerr << " _client.send_produce_async failed " << csi::kafka::to_string(result.ec) << std::endl;
                        // retry after delay - not implemented yet
                        _tx_in_progress = false;
                        _try_send();
                        return;
                    }

                    {
                        csi::kafka::spinlock::scoped_lock xxx(_spinlock);
                        {
                            assert(_tx_queue.size() >= items_in_batch);
                            size_t tx_size = 0;
                            for (size_t i = 0; i != items_in_batch; ++i)
                            {
                                tx_size += (*(_tx_queue.end() - 1))->size();
                                _tx_queue.pop_back();
                            }
                            _tx_queue_byte_size -= tx_size;
                            _metrics_total_tx_kb += tx_size;
                            _metrics_total_tx_msg += items_in_batch;
                        }
                    }

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
