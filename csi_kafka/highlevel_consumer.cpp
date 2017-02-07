#ifdef WIN32 
#define BOOST_ASIO_ERROR_CATEGORY_NOEXCEPT noexcept(true)
#endif

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include "highlevel_consumer.h"
#include <csi-async/async.h>
#include <csi-async/destructor_callback.h>

namespace csi {
namespace kafka {
highlevel_consumer::highlevel_consumer(boost::asio::io_service& io_service, const std::string& topic, int32_t rx_timeout, size_t max_packet_size)
  : _ios(io_service)
  , _timer(io_service)
  , _timeout(boost::posix_time::milliseconds(5000))
  , _meta_client(io_service, topic)
  , _consumer_meta_client(io_service, topic)
  , _topic(topic)
  , _rx_timeout(rx_timeout)
  , _max_packet_size(max_packet_size) {
  // this is an absurd small max size
  assert(max_packet_size > 4);
}

highlevel_consumer::highlevel_consumer(boost::asio::io_service& io_service, const std::string& topic, const std::vector<int>& partion_mask, int32_t rx_timeout, size_t max_packet_size) :
  _ios(io_service),
  _timer(io_service),
  _timeout(boost::posix_time::milliseconds(5000)),
  _meta_client(io_service, topic),
  _consumer_meta_client(io_service, topic),
  _topic(topic),
  _rx_timeout(rx_timeout),
  _max_packet_size(max_packet_size),
  _partitions_mask(partion_mask) {
  // this is an absurd small max size
  assert(max_packet_size > 4);
}

highlevel_consumer::~highlevel_consumer() {
  _timer.cancel();
}

void highlevel_consumer::handle_timer(const boost::system::error_code& ec) {
  if (!ec)
    _try_connect_brokers();
}

void highlevel_consumer::close() {
  _timer.cancel();
  _meta_client.close();
  _consumer_meta_client.close();
  for (std::map<int, lowlevel_consumer*>::iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i) {
    i->second->close("highlevel_consumer::close()");
  }
}

//// TBD should this be unimplemented???
//void highlevel_consumer::connect_forever(const std::vector<broker_address>& brokers) {
//  //meta_client.connect_async(brokers, [this](const boost::system::error_code& ec) {
//  //  _ios.post([this] { _try_connect_brokers(); });
//  //});
//}

//void highlevel_consumer::connect_forever(std::string brokers) {
//  connect_forever(string_to_brokers(brokers));
//}

void highlevel_consumer::connect_async(const std::vector<broker_address>& brokers, int32_t timeout, connect_callback cb) {

  BOOST_LOG_TRIVIAL(trace) << "highlevel_consumer connect_async START";
  _meta_client.connect_async(brokers, timeout, [this, cb](const boost::system::error_code& ec) {
    if (ec) {
      BOOST_LOG_TRIVIAL(debug) << "highlevel_consumer::connect_async failed " << to_string(ec);
      cb(ec);
      return;
    }
    BOOST_LOG_TRIVIAL(trace) << "highlevel_consumer _connect_async STARTING";
    _connect_async(cb);
  }); //connect_async
}

void highlevel_consumer::_connect_async(connect_callback cb) {
  _meta_client.get_metadata_async({_topic}, [this, cb](rpc_result<metadata_response> result) {
    BOOST_LOG_TRIVIAL(trace) << "highlevel_consumer _meta_client.get_metadata_async() CALLBACK ENTERED...";
    handle_response(result);
    if (!result) {
      //std::vector < boost::function <void( const boost::system::error_code&)>> f;
      //vector of async functions having callback on completion

      auto work(std::make_shared<csi::async::work<boost::system::error_code>>(csi::async::PARALLEL, csi::async::ALL));
      for (std::map<int, lowlevel_consumer*>::iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i) {
        if (!i->second->is_connected() && !i->second->is_connection_in_progress()) {
          work->push_back([this, i](csi::async::work<boost::system::error_code>::callback cb) {
            int partition = i->first;
            int leader = _partition2partitions[partition].leader;
            auto bd = _broker2brokers[leader];
            broker_address broker_addr(bd.host_name, bd.port);

            i->second->connect_async(broker_addr, 1000, [this, leader, partition, broker_addr, cb](const boost::system::error_code& ec1) {
              if (ec1) {
                BOOST_LOG_TRIVIAL(warning) << _topic << ":" << partition << ", highlevel_consumer can't connect to broker #" << leader << " (" << to_string(broker_addr) << ") ec:" << ec1;
              } else {
                BOOST_LOG_TRIVIAL(info) << _topic << ":" << partition << ", highlevel_consumer connected to broker #" << leader << " (" << to_string(broker_addr) << ")";
              }
              cb(ec1);
            });
          });
        }
      }

      BOOST_LOG_TRIVIAL(trace) << "highlevel_consumer connect_async / PARALLEL START";
      (*work)([work, cb](const boost::system::error_code& ec) // add iterator for last function
      {
        BOOST_LOG_TRIVIAL(trace) << "highlevel_consumer connect_async / PARALLEL CB ec=" << ec;
        if (ec) {
          BOOST_LOG_TRIVIAL(warning) << "highlevel_consumer connect_async can't connect to broker ec:" << ec;
        }
        cb(ec);
        BOOST_LOG_TRIVIAL(trace) << "highlevel_consumer connect_async / PARALLEL EXIT";
      }); //waterfall
      BOOST_LOG_TRIVIAL(trace) << "highlevel_consumer _meta_client.get_metadata_async() CB EXIT";
    } // get_metadata_async ok?
    else {
      BOOST_LOG_TRIVIAL(trace) << "highlevel_consumer _meta_client.get_metadata_async() error cb";
      cb(result.ec.ec1);
    }
  });
}

boost::system::error_code highlevel_consumer::connect(const std::vector<broker_address>& brokers, int32_t timeout) {
  std::promise<boost::system::error_code> p;
  std::future<boost::system::error_code>  f = p.get_future();
  connect_async(brokers, timeout, [&p](const boost::system::error_code& error) {
    p.set_value(error);
  });
  f.wait();
  boost::system::error_code ec = f.get();
  if (ec) {
    BOOST_LOG_TRIVIAL(warning) << _topic << ", highlevel_consumer can't connect to broker all brokers " << ec.message();
  } else {
    BOOST_LOG_TRIVIAL(info) << _topic << ", highlevel_consumer connect to all brokers OK";
  }
  return ec;
}

boost::system::error_code highlevel_consumer::connect(std::string brokers, int32_t timeout) {
  return connect(string_to_brokers(brokers), timeout);
}

void highlevel_consumer::set_offset(int64_t start_time) {
  // return value??? TBD what to do if this fails and if # partitions changes???
  for (std::map<int, lowlevel_consumer*>::iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i) {
    i->second->set_offset_time(start_time);
  }
}

// we should probably have a good return value here.. a map of partiones to ec ?
void highlevel_consumer::set_offset(const std::map<int32_t, int64_t>& offsets) {
  for (std::map<int, int64_t>::const_iterator i = offsets.begin(); i != offsets.end(); ++i) {
    std::map<int, lowlevel_consumer*>::iterator item = _partition2consumers.find(i->first);
    if (item != _partition2consumers.end()) {
      item->second->set_offset(i->second);
    }
  }
}

std::map<int32_t, int64_t> highlevel_consumer::get_next_offset() const {
  std::map<int32_t, int64_t> res;
  for (std::map<int, lowlevel_consumer*>::const_iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i)
    res[i->first] = i->second->get_next_offset();
  return res;
}

// we should probably have a good return value here.. a map of partiones to ec ?
void highlevel_consumer::set_offset(const std::vector<topic_offset>& offsets) {
  for (std::vector<topic_offset>::const_iterator i = offsets.begin(); i != offsets.end(); ++i) {
    std::map<int, lowlevel_consumer*>::iterator item = _partition2consumers.find(i->partition);
    if (item != _partition2consumers.end()) {
      item->second->set_offset(i->offset);
    }
  }
}

void highlevel_consumer::_try_connect_brokers() {
  _connect_async([this](const boost::system::error_code& ec) {
    _timer.expires_from_now(_timeout);
    _timer.async_wait(boost::bind(&highlevel_consumer::handle_timer, this, boost::asio::placeholders::error));
  });
}

void highlevel_consumer::handle_response(rpc_result<metadata_response> result) {
  if (!result) {
    {
      csi::spinlock::scoped_lock xxx(_spinlock);

      if (_partition2consumers.size() == 0) {
        for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = result->topics.begin(); i != result->topics.end(); ++i) {
          assert(i->topic_name == _topic);
          if (i->error_code) {
            BOOST_LOG_TRIVIAL(warning) << _topic << ", highlevel_consumer get metadata failed: " << to_string((error_codes) i->error_code);
          }
          for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j) {
            if (_partitions_mask.size() == 0) {
              _partition2consumers.insert(std::make_pair(j->partition_id, new lowlevel_consumer(_ios, _topic, j->partition_id, _rx_timeout, _max_packet_size)));
            } else if (std::find(std::begin(_partitions_mask), std::end(_partitions_mask), j->partition_id) != std::end(_partitions_mask)) {
              _partition2consumers.insert(std::make_pair(j->partition_id, new lowlevel_consumer(_ios, _topic, j->partition_id, _rx_timeout, _max_packet_size)));
            }
          }
        };
      }

      for (std::vector<csi::kafka::broker_data>::const_iterator i = result->brokers.begin(); i != result->brokers.end(); ++i) {
        _broker2brokers[i->node_id] = *i;
      };

      for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = result->topics.begin(); i != result->topics.end(); ++i) {
        assert(i->topic_name == _topic);
        for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j) {
          _partition2partitions[j->partition_id] = *j;
        };
      };
    }
  }
}

void  highlevel_consumer::stream_async(datastream_callback cb) {
  for (std::map<int, lowlevel_consumer*>::const_iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i) {
    i->second->stream_async(cb);
  }
}

void highlevel_consumer::pause() {
  for (std::map<int, lowlevel_consumer*>::const_iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i) {
    i->second->pause();
  }
}

void highlevel_consumer::resume() {
  for (std::map<int, lowlevel_consumer*>::const_iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i) {
    i->second->resume();
  }
}

void highlevel_consumer::fetch(fetch_callback cb) {
  auto final_cb = std::make_shared<csi::async::destructor_callback<std::vector<rpc_result<csi::kafka::fetch_response>>>>(cb);
  for (std::map<int, lowlevel_consumer*>::const_iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i) {
    i->second->fetch([final_cb](rpc_result<csi::kafka::fetch_response> response) {
      final_cb->value().push_back(response);
    });
  }
}

std::vector<rpc_result<csi::kafka::fetch_response>> highlevel_consumer::fetch() {
  std::promise<std::vector<rpc_result<csi::kafka::fetch_response>>> p;
  std::future<std::vector<rpc_result<csi::kafka::fetch_response>>>  f = p.get_future();
  fetch([&p](std::vector<rpc_result<csi::kafka::fetch_response>> res) {
    p.set_value(res);
  });
  f.wait();
  return f.get();
}

std::vector<highlevel_consumer::metrics>  highlevel_consumer::get_metrics() const {
  std::vector<metrics> metrics;
  for (std::map<int, lowlevel_consumer*>::const_iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i) {
    highlevel_consumer::metrics item;
    item.partition = (*i).second->partition();
    item.total_rx_bytes = (*i).second->total_rx_bytes();
    item.total_rx_msg = (*i).second->total_rx_msg();
    metrics.push_back(item);
  }
  return metrics;
}
};
};
