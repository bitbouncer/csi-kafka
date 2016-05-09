#include <algorithm>
#include <boost/crc.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <csi_kafka/internal/async.h>
#include "highlevel_producer.h"

namespace csi {
  namespace kafka {
    highlevel_producer::highlevel_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t required_acks, int32_t tx_timeout, int32_t max_packet_size) :
      _ios(io_service),
      _timer(io_service),
      _timeout(boost::posix_time::milliseconds(5000)),
      _meta_client(io_service, topic),
      _topic(topic),
      _required_acks(required_acks),
      _tx_timeout(tx_timeout),
      _max_packet_size(max_packet_size) {}

    highlevel_producer::~highlevel_producer() {
      _timer.cancel();
      //TBD we lead lowlevelproducers here .... shared_ptr ??
    }

    void highlevel_producer::handle_timer(const boost::system::error_code& ec) {
      if(!ec)
        _try_connect_brokers();
    }

    void highlevel_producer::close() {
      _timer.cancel();
      _meta_client.close();
      for(std::map<int, lowlevel_producer*>::iterator i = _partition2producers.begin(); i != _partition2producers.end(); ++i) {
        i->second->close();
      }
    }

    void highlevel_producer::connect_forever(const std::vector<broker_address>& brokers) {
      _meta_client.connect_async(brokers, [this](const boost::system::error_code& ec) {
        _ios.post([this] { _try_connect_brokers(); });
      });
    }

    void highlevel_producer::connect_async(const std::vector<broker_address>& brokers, connect_callback cb) {
      BOOST_LOG_TRIVIAL(trace) << "highlevel_producer ::connect_async START";
      _meta_client.connect_async(brokers, [this, cb](const boost::system::error_code& ec) {
        BOOST_LOG_TRIVIAL(trace) << "highlevel_producer connect_async CB";
        if(!ec) {
          BOOST_LOG_TRIVIAL(trace) << "highlevel_producer _meta_client.get_metadata_async() STARTING";
          _meta_client.get_metadata_async({ _topic }, [this, cb](rpc_result<metadata_response> result) {
            BOOST_LOG_TRIVIAL(trace) << "highlevel_producer _meta_client.get_metadata_async() CALLBACK ENTERED...";
            handle_response(result);
            if(!result) {
              //std::vector < boost::function <void( const boost::system::error_code&)>> f;
              //vector of async functions having callback on completion

              std::shared_ptr<std::vector<csi::async::async_function>> work(new std::vector<csi::async::async_function>());

              for(std::map<int, lowlevel_producer*>::iterator i = _partition2producers.begin(); i != _partition2producers.end(); ++i) {
                work->push_back([this, i](csi::async::async_callback cb) {
                  int partition = i->first;
                  int leader = _partition2partitions[partition].leader;
                  auto bd = _broker2brokers[leader];
                  broker_address broker_addr(bd.host_name, bd.port);

                  i->second->connect_async(broker_addr, 1000, [this, leader, partition, broker_addr, cb](const boost::system::error_code& ec1) {
                    if(ec1) {
                      BOOST_LOG_TRIVIAL(warning) << _topic << ":" << partition << ", highlevel_producer can't connect to broker #" << leader << " (" << to_string(broker_addr) << ") ec:" << ec1;
                    } else {
                      BOOST_LOG_TRIVIAL(info) << _topic << ":" << partition << ", highlevel_producer connected to broker #" << leader << " (" << to_string(broker_addr) << ")";
                    }
                    cb(ec1);
                  });
                });
              }

              BOOST_LOG_TRIVIAL(trace) << "highlevel_producer connect_async / waterfall START";
              csi::async::waterfall(*work, [work, cb](const boost::system::error_code& ec) // add iterator for last function
              {
                BOOST_LOG_TRIVIAL(trace) << "highlevel_producer connect_async / waterfall CB ec=" << ec;
                if(ec) {
                  BOOST_LOG_TRIVIAL(warning) << "highlevel_producer connect_async can't connect to broker ec:" << ec;
                }
                cb(ec);
                BOOST_LOG_TRIVIAL(trace) << "highlevel_producer connect_async / waterfall EXIT";
              }); //waterfall
              BOOST_LOG_TRIVIAL(trace) << "highlevel_producer _meta_client.get_metadata_async() CB EXIT";
            } // get_metadata_async ok?
            else {
              BOOST_LOG_TRIVIAL(trace) << _topic << ", highlevel_producer _meta_client.get_metadata_async() error cb";
              cb(result.ec.ec1);
            }
          }); // get_metadata_async
        } // connect ok?
      }); //connect_async
    }

    boost::system::error_code highlevel_producer::connect(const std::vector<broker_address>& brokers) {
      BOOST_LOG_TRIVIAL(trace) << _topic << ", highlevel_producer connect START";
      std::promise<boost::system::error_code> p;
      std::future<boost::system::error_code>  f = p.get_future();
      connect_async(brokers, [&p](const boost::system::error_code& error) {
        p.set_value(error);
      });
      f.wait();

      boost::system::error_code ec = f.get();
      if(ec) {
        BOOST_LOG_TRIVIAL(warning) << _topic << ", highlevel_producer can't connect to broker all brokers " << ec.message();
      } else {
        BOOST_LOG_TRIVIAL(info) << _topic << ", highlevel_producer connect to all brokers OK";
      }
      return ec;
    }

    void highlevel_producer::_try_connect_brokers() {
      // the number of partitions is constant but the serving hosts might differ
      _meta_client.get_metadata_async({ _topic }, [this](rpc_result<metadata_response> result) {
        handle_response(result);

        for(std::map<int, lowlevel_producer*>::iterator i = _partition2producers.begin(); i != _partition2producers.end(); ++i) {
          if(!i->second->is_connected() && !i->second->is_connection_in_progress()) {

            int partition = i->first;
            int leader = _partition2partitions[partition].leader;
            auto bd = _broker2brokers[leader];
            broker_address addr(bd.host_name, bd.port);
            i->second->close();
            BOOST_LOG_TRIVIAL(info) << _topic << ":" << partition << ", highlevel_producer connecting to broker node_id:" << leader << " (" << to_string(addr) << ")";
            i->second->connect_async(addr, 1000, [this, leader, partition, addr](const boost::system::error_code& ec1) {
              if(ec1) {
                BOOST_LOG_TRIVIAL(warning) << _topic << ":" << partition << ", highlevel_producer can't connect to broker #" << leader << " (" << to_string(addr) << ") ec:" << ec1;
              } else {
                BOOST_LOG_TRIVIAL(info) << _topic << ":" << partition << ", highlevel_producer connected to broker #" << leader << " (" << to_string(addr) << ")";
              }
            });
          }
        }
        _timer.expires_from_now(_timeout);
        _timer.async_wait(boost::bind(&highlevel_producer::handle_timer, this, boost::asio::placeholders::error));
      });
    }

    void highlevel_producer::handle_response(rpc_result<metadata_response> result) {
      if(!result) {
        {
          csi::kafka::spinlock::scoped_lock xxx(_spinlock);
          //_metadata = result;

          //changes?
          //TODO we need to create new producers if we have more partitions
          //TODO we cant shrink cluster at the moment...
          if(_partition2producers.size() == 0) {
            for(std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = result->topics.begin(); i != result->topics.end(); ++i) {
              assert(i->topic_name == _topic);
              if(i->error_code) {
                BOOST_LOG_TRIVIAL(warning) << _topic << ", highlevel_producer get_metadata failed: " << to_string((error_codes) i->error_code);
              }
              for(std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                _partition2producers.insert(std::make_pair(j->partition_id, new lowlevel_producer(_ios, _topic, j->partition_id, _required_acks, _tx_timeout, _max_packet_size)));
            }

            //lets send all things that were collected before connecting
            std::deque<tx_item> ::reverse_iterator cursor = _tx_queue.rbegin();

            while(_tx_queue.size()) {
              tx_item& item = *(_tx_queue.end() - 1);
              uint32_t partition = item.hash % _partition2producers.size();
              _partition2producers[partition]->send_async(item.msg, item.cb);
              _tx_queue.pop_back();
            }
          }


          for(std::vector<csi::kafka::broker_data>::const_iterator i = result->brokers.begin(); i != result->brokers.end(); ++i) {
            _broker2brokers[i->node_id] = *i;
          };

          for(std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = result->topics.begin(); i != result->topics.end(); ++i) {
            assert(i->topic_name == _topic);
            for(std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j) {
              _partition2partitions[j->partition_id] = *j;
            };
          };
        }
      }
    }

    void highlevel_producer::send_async(uint32_t partition_hash, std::shared_ptr<csi::kafka::basic_message> message, tx_ack_callback cb)
    {
      // enqueu in partition queue or store if we don't have a connection the cluster.
      csi::kafka::spinlock::scoped_lock xxx(_spinlock);

      // TBD change below when we can accept repartitioning - for now it's okay to store initial data and send it when we find the cluster.
      //if (_meta_client.is_connected() && _partition2producers.size())
      if(_partition2producers.size()) {
        uint32_t partition = partition_hash % _partition2producers.size();
        _partition2producers[partition]->send_async(message, cb);
      } else {
        _tx_queue.push_front(tx_item(partition_hash, message, cb));
      }
    }

    int32_t highlevel_producer::send_sync(uint32_t partition_hash, std::shared_ptr<csi::kafka::basic_message> message) {
      std::promise<int32_t> p;
      std::future<int32_t>  f = p.get_future();
      send_async(partition_hash, message, [&p](int32_t result) {
        p.set_value(result);
      });
      f.wait();
      return f.get();
    }

    // if you give a partition id in message it will end up there - otherwise calc a crc32 on key and place it in hash % nr_of_partitions
    void highlevel_producer::send_async(std::shared_ptr<basic_message> message, tx_ack_callback cb) {
      uint32_t hash = 0;
      if(!message->key.is_null()) {
        // calc a hash to get partition
        boost::crc_32_type result;
        uint32_t keysize = (uint32_t) message->key.size();
        result.process_bytes(&message->key[0], message->key.size());
        hash = result.checksum();
      } else if(!message->value.is_null()) {
        // calc a hash to get partition
        boost::crc_32_type result;
        uint32_t valsize = (uint32_t) message->value.size();
        result.process_bytes(&message->value[0], valsize);
        hash = result.checksum();
      }
      send_async(hash, message, cb);
    }

    void highlevel_producer::send_async(std::vector<std::shared_ptr<basic_message>>& messages, tx_ack_callback cb) {
      for(std::vector<std::shared_ptr<basic_message>>::const_iterator i = messages.begin(); i != messages.end(); ++i)
        send_async(*i);
      size_t partitions = _partition2producers.size();
      auto final_cb = std::make_shared<csi::async::destructor_callback<uint32_t>>(cb);
      final_cb->value() = 0;  // initialize ec....
      for(int i = 0; i != partitions; ++i) {
        _partition2producers[i]->send_async(NULL, [i, final_cb](int32_t ec) {
          if(ec) {
            final_cb->value() = ec;
          }
        });
      }
    }

    int32_t highlevel_producer::send_sync(std::shared_ptr<csi::kafka::basic_message> message) {
      std::promise<int32_t> p;
      std::future<int32_t>  f = p.get_future();
      send_async(message, [&p](int32_t result) {
        p.set_value(result);
      });
      f.wait();
      return f.get();
    }

    int32_t highlevel_producer::send_sync(std::vector<std::shared_ptr<csi::kafka::basic_message>>& messages) {
      std::promise<int32_t> p;
      std::future<int32_t>  f = p.get_future();
      send_async(messages, [&p](int32_t result) {
        p.set_value(result);
      });
      f.wait();
      return f.get();
    }
      
    size_t highlevel_producer::items_in_queue() const {
      size_t items = 0;
      items += _tx_queue.size();
      csi::kafka::spinlock xx;
      for(std::map<int, lowlevel_producer*>::const_iterator i = _partition2producers.begin(); i != _partition2producers.end(); ++i)
        items += i->second->items_in_queue();
      return items;
    };

    std::vector<highlevel_producer::metrics>  highlevel_producer::get_metrics() const {
      std::vector<metrics> metrics;
      for(std::map<int, lowlevel_producer*>::const_iterator i = _partition2producers.begin(); i != _partition2producers.end(); ++i) {
        highlevel_producer::metrics item;
        item.partition = (*i).second->partition();
        item.bytes_in_queue = (*i).second->bytes_in_queue();
        item.msg_in_queue = (*i).second->items_in_queue();
        item.tx_kb_sec = (*i).second->metrics_kb_sec();
        item.tx_msg_sec = (*i).second->metrics_msg_sec();
        item.tx_roundtrip = (*i).second->metrics_tx_roundtrip();
        metrics.push_back(item);
      }
      return metrics;
    }
  };
};
