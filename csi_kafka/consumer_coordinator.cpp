#ifdef WIN32 
#define BOOST_ASIO_ERROR_CATEGORY_NOEXCEPT noexcept(true)
#endif

#include <thread>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/bind.hpp>
#include "consumer_coordinator.h"
#include <csi-async/async.h>
#include <csi_kafka/kafka.h>

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

namespace csi {
namespace kafka {
consumer_coordinator::consumer_coordinator(boost::asio::io_service& io_service, const std::string& consumer_group)
  : _ios(io_service)
  , _client(io_service)
  , _consumer_group(consumer_group) {}

consumer_coordinator::~consumer_coordinator() {
  _client.close();
}

// a bit complicated to know where to connect....
void consumer_coordinator::connect_async(const std::vector<broker_address>& brokers, int32_t timeout, connect_callback done_cb) {
  _initial_brokers = brokers;

  // move connect any to client. ???
  // we manipulate the same shared _client so we nned to do it sequetially
  auto work(std::make_shared<csi::async::work<boost::system::error_code>>(csi::async::SEQUENTIAL, csi::async::FIRST_SUCCESS));
  for (auto const& i : brokers) {
    work->push_back([this, timeout, i](csi::async::work<boost::system::error_code>::callback cb) {
      _client.connect_async(i, timeout, [this, timeout, cb](boost::system::error_code ec) {
        if (ec)
          return cb(ec);
        boost::system::error_code ignored;
        BOOST_LOG_TRIVIAL(debug) << _consumer_group << ", consumer_coordinator, connected to cluster OK (" << _client.remote_endpoint(ignored).address().to_string() << ")";

        _client.get_group_coordinator_async(_consumer_group, [this, timeout, cb](rpc_result<group_coordinator_response> result) {
          if (result) // no matter the error - let's fake the error code
          {
            BOOST_LOG_TRIVIAL(error) << _consumer_group << ", consumer_coordinator, get_group_coordinator failed: " << to_string((csi::kafka::error_codes) result->error_code);
            cb(make_error_code(boost::system::errc::host_unreachable));
            return;
          }
          csi::kafka::broker_address coordinator_address(result.data->coordinator_host, result.data->coordinator_port);
          BOOST_LOG_TRIVIAL(debug) << _consumer_group << ", consumer_coordinator, get_group_coordinator OK (" << to_string(coordinator_address) << ")";
          _client.connect_async(coordinator_address, timeout, [this, cb](const boost::system::error_code& ec) {
            boost::system::error_code ignored;
            if (ec)
              BOOST_LOG_TRIVIAL(error) << _consumer_group << ", consumer_coordinator, failed to connect to group coordinator" << to_string(ec);
            else
              BOOST_LOG_TRIVIAL(debug) << _consumer_group << ", consumer_coordinator, connected to group coordinator (" << _client.remote_endpoint(ignored).address().to_string() << ")";
            cb(ec);
          }); // connect_async
        }); // get_group_coordinator_async
      }); // connect
    }); // work
  } // for brokers

  (*work)([work, this, timeout, done_cb](boost::system::error_code ec) {
    BOOST_LOG_TRIVIAL(error) << _consumer_group << ", consumer_coordinator, work completen################";
    if (ec)
      BOOST_LOG_TRIVIAL(error) << _consumer_group << ", consumer_coordinator, failed to connect to cluster: " << to_string(ec);
    done_cb(ec);
  });
}

boost::system::error_code consumer_coordinator::connect(const std::vector<broker_address>& brokers, int32_t timeout) {
  std::promise<boost::system::error_code> p;
  std::future<boost::system::error_code>  f = p.get_future();
  connect_async(brokers, timeout, [&p](boost::system::error_code error) {
    p.set_value(error);
  });
  f.wait();
  return f.get();
}

boost::system::error_code consumer_coordinator::connect(std::string brokers, int32_t timeout) {
  return connect(string_to_brokers(brokers), timeout);
}

void consumer_coordinator::close() {
  _client.close();
}

void consumer_coordinator::get_consumer_offset_async(std::string topic, get_consumer_offset_callback cb) {
  _client.get_consumer_offset_async(_consumer_group, topic, cb);
}

rpc_result<offset_fetch_response> consumer_coordinator::get_consumer_offset(std::string topic) {
  return _client.get_consumer_offset(_consumer_group, topic);
}

void consumer_coordinator::get_consumer_offset_async(std::string topic, int32_t partition, get_consumer_offset_callback cb) {
  _client.get_consumer_offset_async(_consumer_group, topic, partition, cb);
}

rpc_result<offset_fetch_response> consumer_coordinator::get_consumer_offset(std::string topic, int32_t partition) {
  return _client.get_consumer_offset(_consumer_group, topic, partition);
}

void consumer_coordinator::commit_consumer_offset_async(
  int32_t consumer_group_generation_id,
  const std::string& consumer_id,
  std::string topic,
  const std::vector<topic_offset>& offsets,
  const std::string& metadata,
  commit_offset_callback cb) {
  _client.commit_consumer_offset_async(_consumer_group, consumer_group_generation_id, consumer_id, topic, offsets, metadata, cb);
}

rpc_result<offset_commit_response> consumer_coordinator::commit_consumer_offset(
  int32_t consumer_group_generation_id,
  const std::string& consumer_id,
  std::string topic,
  const std::vector<topic_offset>& offsets,
  const std::string& metadata) {
  return _client.commit_consumer_offset(_consumer_group, consumer_group_generation_id, consumer_id, topic, offsets, metadata);
}

void consumer_coordinator::commit_consumer_offset_async(
  int32_t consumer_group_generation_id,
  const std::string& consumer_id,
  std::string topic,
  const std::map<int32_t, int64_t>& offsets,
  const std::string& metadata,
  commit_offset_callback cb) {
  _client.commit_consumer_offset_async(_consumer_group, consumer_group_generation_id, consumer_id, topic, offsets, metadata, cb);
}

rpc_result<offset_commit_response> consumer_coordinator::commit_consumer_offset(
  int32_t consumer_group_generation_id,
  const std::string& consumer_id,
  std::string topic,
  const std::map<int32_t, int64_t>& offsets,
  const std::string& metadata) {
  return _client.commit_consumer_offset(_consumer_group, consumer_group_generation_id, consumer_id, topic, offsets, metadata);
}

/*
std::vector<topic_offset> parse(rpc_result<offset_fetch_response> result, std::string topic, int32_t& ec) {
  ec = 0;
  std::vector<topic_offset>  r;
  for (std::vector<offset_fetch_response::topic_data>::const_iterator i = result->topics.begin(); i != result->topics.end(); ++i)
  {
    if (i->topic_name == topic)
    {
      for (std::vector<offset_fetch_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
      {
        if (j->error_code)
        {
          ec = j->error_code;
        } else
        {
          r.emplace_back(j->partition_id, j->offset);
        }
      }
    }
  }
  return r;
}
*/

std::map<int32_t, int64_t> parse(rpc_result<offset_fetch_response> result, std::string topic, int32_t& ec) {
  ec = 0;
  std::map<int32_t, int64_t>  r;
  for (auto const & i : result->topics) {
    if (i.topic_name == topic) {
      for (auto const & j : i.partitions) {
        if (j.error_code) {
          ec = j.error_code;
        } else {
          r[j.partition_id] = j.offset;
        }
      }
    }
  }
  return r;
}


} // kafka
}; // csi
