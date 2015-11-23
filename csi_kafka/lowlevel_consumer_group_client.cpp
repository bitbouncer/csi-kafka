#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include "lowlevel_consumer_group_client.h"

namespace csi
{
    namespace kafka
    {
        lowlevel_consumer_group_client::lowlevel_consumer_group_client(boost::asio::io_service& io_service, const std::string& topic, const std::string& consumer_group, int32_t partition, int32_t rx_timeout) :
            _ios(io_service),
            _client(io_service),
            _topic(topic),
            _consumer_group(consumer_group),
            _partition(partition),
            _rx_timeout(rx_timeout),
            _rx_in_progress(false),
            _transient_failure(false)
        {
        }

        lowlevel_consumer_group_client::~lowlevel_consumer_group_client()
        {
            _client.close();
        }

        void lowlevel_consumer_group_client::connect_async(const broker_address& address, int32_t timeout, connect_callback cb)
        {
            _client.connect_async(address, timeout, cb);
        }

        boost::system::error_code lowlevel_consumer_group_client::connect(const broker_address& address, int32_t timeout)
        {
            return _client.connect(address, timeout);
        }


        void lowlevel_consumer_group_client::connect_async(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout, connect_callback cb)
        {
            _client.connect_async(query, timeout, cb);
        }

        boost::system::error_code lowlevel_consumer_group_client::connect(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout)
        {
            return _client.connect(query, timeout);
        }

        void lowlevel_consumer_group_client::close()
        {
            _client.close();
        }

        void lowlevel_consumer_group_client::get_metadata_async(get_metadata_callback cb)
        {
            _client.get_metadata_async({ _topic }, 0, cb);
        }

        rpc_result<metadata_response> lowlevel_consumer_group_client::get_metadata()
        {
            return _client.get_metadata({ _topic }, 0);
        }

        void lowlevel_consumer_group_client::get_consumer_metadata_async(int32_t correlation_id, get_consumer_metadata_callback cb)
        {
            _client.get_consumer_metadata_async(_consumer_group, correlation_id, cb);
        }

        rpc_result<consumer_metadata_response> lowlevel_consumer_group_client::get_consumer_metadata(int32_t correlation_id)
        {
            return _client.get_consumer_metadata(_consumer_group, correlation_id);
        }


        void lowlevel_consumer_group_client::get_consumer_offset_async(int32_t correlation_id, get_consumer_offset_callback cb)
        {
            _client.get_consumer_offset_async(_consumer_group, _topic, _partition, correlation_id, cb);
        }

        rpc_result<offset_fetch_response> lowlevel_consumer_group_client::get_consumer_offset(int32_t correlation_id)
        {
            return _client.get_consumer_offset(_consumer_group, _topic, _partition, correlation_id);
        }

        void lowlevel_consumer_group_client::commit_consumer_offset_async(
            int32_t consumer_group_generation_id,
            const std::string& consumer_id,
            int64_t offset,
            const std::string& metadata,
            int32_t correlation_id,
            commit_offset_callback cb)
        {
            _client.commit_consumer_offset_async(_consumer_group, consumer_group_generation_id, consumer_id, _topic, _partition, offset, metadata, correlation_id, cb);
        }

        rpc_result<offset_commit_response> lowlevel_consumer_group_client::commit_consumer_offset(
            int32_t consumer_group_generation_id,
            const std::string& consumer_id,
            int64_t offset,
            const std::string& metadata,
            int32_t correlation_id)
        {
            return _client.commit_consumer_offset(_consumer_group, consumer_group_generation_id, consumer_id, _topic, _partition, offset, metadata, correlation_id);
        }
    } // kafka
}; // csi
