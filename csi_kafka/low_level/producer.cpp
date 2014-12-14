#include "producer.h"
#include <boost/thread.hpp>
#include <boost/bind.hpp>

namespace csi
{
    namespace kafka
    {
        producer::producer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic, int32_t partition) :
            _ios(io_service),
            _client(io_service, query),
            _topic_name(topic),
            _partition_id(partition)
        {
        }

        void producer::connect_async(connect_callback cb)
        {
            _client.connect_async(cb);
        }

        boost::system::error_code producer::connect()
        {
            return _client.connect();
        }

        void  producer::close()
        {
            _client.close();
        }

        void producer::send_async(int32_t required_acks, int32_t timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, send_callback cb)
        {
            _client.send_produce_async(_topic_name, _partition_id, required_acks, timeout, v, correlation_id, cb);
        }



        async_producer::async_producer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic, int32_t partition) :
                _ios(io_service),
                _client(io_service, query),
                _topic_name(topic),
                _partition_id(partition),
                _tx_queue_byte_size(0),
                _tx_in_progress(false),
                _required_acks(-1),
                _tx_timeout(500)
                {
                }

        void async_producer::connect_async(connect_callback cb)
        {
            _client.connect_async(cb);
        }

        boost::system::error_code async_producer::connect()
        {
            return _client.connect();
        }

        void  async_producer::close()
        {
            _client.close();
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

            _ios.post([this](){_try_send();});



            csi::kafka::spinlock::scoped_lock xxx(_spinlock);
        }

        void async_producer::_try_send()
        {
            if (_tx_in_progress)
                return;

            size_t items_in_batch = 0;
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
                        items_in_batch++;
                        v.push_back(*cursor);
                    }
                }
            }

            _client.send_produce_async(_topic_name, _partition_id, _required_acks, _tx_timeout, v, 99, [this, items_in_batch](rpc_result<produce_response> result)
            {
                if (result)
                {
                    std::cerr << " _client.send_produce_async failed" << std::endl;
                    // retry after delay - not implemented yet
                    _tx_in_progress = false;
                    _try_send();
                    return;
                }

                {
                    csi::kafka::spinlock::scoped_lock xxx(_spinlock);
                    {
                        for (size_t i = 0; i != items_in_batch; ++i)
                        {
                            _tx_queue_byte_size -= (*_tx_queue.rend())->size();
                            _tx_queue.pop_back();
                        }
                    }
                }

                _tx_in_progress = false; // here is a small gap where an external enqueu would trigger a post(_try_send()) - thus callinmg twice but it should be harmless
                _try_send();
            });
        }
    } // kafka
}; // csi
