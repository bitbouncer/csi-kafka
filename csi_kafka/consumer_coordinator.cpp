#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include "consumer_coordinator.h"

/*
* https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Detailed+Consumer+Coordinator+Design
*
*
* consumerStartup (initBrokers : Map[Int, (String, String)]):
*  
* 1. In a round robin fashion, pick a broker in the initialized cluster metadata, create a socket channel with that broker
*  
* 1.1. If the socket channel cannot be established, it will log an error and try the next broker in the initBroker list
*  
* 1.2. The consumer will keep retrying connection to the brokers in a round robin fashioned it is shut down.
*  
* 2. Send a ClusterMetadataRequest request to the broker and get a ClusterMetadataResponse from the broker
*  
* 3. From the response update its local memory of the current server cluster metadata and the id of the current coordinator
*  
* 4. Set up a socket channel with the current coordinator, send a RegisterConsumerRequest and receive a RegisterConsumerResponse
* 
* 5. If the RegisterConsumerResponse indicates the consumer registration is successful, 
*    it will try to keep reading rebalancing requests from the channel; otherwise go back to step 1
*
*/

namespace csi
{
    namespace kafka
    {
        consumer_coordinator::consumer_coordinator(boost::asio::io_service& io_service, const std::string& topic, const std::string& consumer_group, int32_t partition, int32_t rx_timeout) :
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

        consumer_coordinator::~consumer_coordinator()
        {
            _client.close();
        }

        void consumer_coordinator::connect_async(const broker_address& address, int32_t timeout, connect_callback cb)
        {
            _client.connect_async(address, timeout, cb);
        }

        boost::system::error_code consumer_coordinator::connect(const broker_address& address, int32_t timeout)
        {
            return _client.connect(address, timeout);
        }


        void consumer_coordinator::connect_async(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout, connect_callback cb)
        {
            _client.connect_async(query, timeout, cb);
        }

        boost::system::error_code consumer_coordinator::connect(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout)
        {
            return _client.connect(query, timeout);
        }

        void consumer_coordinator::close()
        {
            _client.close();
        }

        void consumer_coordinator::get_metadata_async(get_metadata_callback cb)
        {
            _client.get_metadata_async({ _topic }, 0, cb);
        }

        rpc_result<metadata_response> consumer_coordinator::get_metadata()
        {
            return _client.get_metadata({ _topic }, 0);
        }

        void consumer_coordinator::get_cluster_metadata_async(int32_t correlation_id, get_cluster_metadata_callback cb)
        {
            _client.get_cluster_metadata_async(_consumer_group, correlation_id, cb);
        }

        rpc_result<cluster_metadata_response> consumer_coordinator::get_cluster_metadata(int32_t correlation_id)
        {
            return _client.get_cluster_metadata(_consumer_group, correlation_id);
        }


        void consumer_coordinator::get_consumer_offset_async(int32_t correlation_id, get_consumer_offset_callback cb)
        {
            _client.get_consumer_offset_async(_consumer_group, _topic, _partition, correlation_id, cb);
        }

        rpc_result<offset_fetch_response> consumer_coordinator::get_consumer_offset(int32_t correlation_id)
        {
            return _client.get_consumer_offset(_consumer_group, _topic, _partition, correlation_id);
        }

        void consumer_coordinator::commit_consumer_offset_async(
            int32_t consumer_group_generation_id,
            const std::string& consumer_id,
            int64_t offset,
            const std::string& metadata,
            int32_t correlation_id,
            commit_offset_callback cb)
        {
            _client.commit_consumer_offset_async(_consumer_group, consumer_group_generation_id, consumer_id, _topic, _partition, offset, metadata, correlation_id, cb);
        }

        rpc_result<offset_commit_response> consumer_coordinator::commit_consumer_offset(
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
