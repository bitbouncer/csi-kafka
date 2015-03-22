#include "lowlevel_producer.h"
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>


namespace csi
{
    namespace kafka
    {
        lowlevel_producer::lowlevel_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition) :
            _ios(io_service),
            _client(io_service),
            _topic(topic),
            _partition_id(partition)
        {
        }

        void lowlevel_producer::connect_async(const broker_address& address, int32_t timeout, connect_callback cb)
        {
            _client.connect_async(address, timeout, cb);
        }

        boost::system::error_code lowlevel_producer::connect(const broker_address& address, int32_t timeout)
        {
            return _client.connect(address, timeout);
        }

        void lowlevel_producer::connect_async(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout, connect_callback cb)
        {
            _client.connect_async(query, timeout, cb);
        }

        boost::system::error_code lowlevel_producer::connect(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout)
        {
            return _client.connect(query, timeout);
        }

        void  lowlevel_producer::close()
        {
            _client.close();
        }

        void lowlevel_producer::send_async(int32_t required_acks, int32_t timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, send_callback cb)
        {
            _client.send_produce_async(_topic, _partition_id, required_acks, timeout, v, correlation_id, cb);
        }
    } // kafka
}; // csi
