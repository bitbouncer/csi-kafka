#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include "lowlevel_consumer.h"

namespace csi {
  namespace kafka {
    lowlevel_consumer::lowlevel_consumer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition, int32_t rx_timeout, size_t max_packet_size) :
      _ios(io_service),
      _client(io_service),
      _topic(topic),
      _partition(partition),
      _max_packet_size(max_packet_size),
      _next_offset(kafka::latest_offsets),
      _rx_timeout(rx_timeout),
      _rx_in_progress(false),
      _transient_failure(false),
      _pause_streaming(false),
      _metrics_rx_kb_sec(boost::accumulators::tag::rolling_window::window_size = 10),
      _metrics_rx_msg_sec(boost::accumulators::tag::rolling_window::window_size = 10),
      _metrics_rx_roundtrip(boost::accumulators::tag::rolling_window::window_size = 10),
      _metrics_timer(io_service),
      _metrics_timeout(boost::posix_time::milliseconds(1000)),
      _metrics_total_rx_kb(0),
      _metrics_total_rx_msg(0),
      __metrics_last_total_rx_kb(0),
      __metrics_last_total_rx_msg(0) {
      _metrics_timer.expires_from_now(_metrics_timeout);
      _metrics_timer.async_wait([this](const boost::system::error_code& ec) { handle_metrics_timer(ec); });
    }

    lowlevel_consumer::~lowlevel_consumer() {
      _client.close();
      _metrics_timer.cancel();
    }

    void lowlevel_consumer::handle_metrics_timer(const boost::system::error_code& ec) {
      if(ec)
        return;

      // todo meassure the actual time  - do not assume 1000
      uint64_t kb_sec = (_metrics_total_rx_kb - __metrics_last_total_rx_kb) / 1024;
      uint64_t msg_sec = (_metrics_total_rx_msg - __metrics_last_total_rx_msg);
      _metrics_rx_kb_sec((double) kb_sec);
      _metrics_rx_msg_sec((double) msg_sec);
      __metrics_last_total_rx_kb = _metrics_total_rx_kb;
      __metrics_last_total_rx_msg = _metrics_total_rx_msg;
      _metrics_timer.expires_from_now(_metrics_timeout);
      _metrics_timer.async_wait([this](const boost::system::error_code& ec) { handle_metrics_timer(ec); });

      if(_transient_failure) {
        _transient_failure = false;
        //_client.get-address() << 
        boost::system::error_code ignored;
        BOOST_LOG_TRIVIAL(info) << "LLC " << _client.remote_endpoint(ignored).address().to_string() << " " << _topic << ":" << _partition << " retrying fetch";
        //LOG restartring fetch
      }

      _ios.post([this]() {_try_fetch(); }); // this will result in a delay of <5 sec connect before actuallt streaming restarts - harmless or we have to catch conmnect callback and retry from there
    }

    void lowlevel_consumer::connect_async(const broker_address& address, int32_t timeout, connect_callback cb) {
      _client.connect_async(address, timeout, cb);
    }

    boost::system::error_code lowlevel_consumer::connect(const broker_address& address, int32_t timeout) {
      return _client.connect(address, timeout);
    }


    void lowlevel_consumer::connect_async(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout, connect_callback cb) {
      _client.connect_async(query, timeout, cb);
    }

    boost::system::error_code lowlevel_consumer::connect(const boost::asio::ip::tcp::resolver::query& query, int32_t timeout) {
      return _client.connect(query, timeout);
    }

    void lowlevel_consumer::close(std::string reason) {
      boost::system::error_code ignored;
      BOOST_LOG_TRIVIAL(debug) << "LLC " << _client.remote_endpoint(ignored).address().to_string() << " " << _topic << ":" << _partition << " closing, reason: " << reason;
      _client.close();
    }

    void lowlevel_consumer::set_offset_time_async(int64_t start_time, set_offset_callback cb) {
      _client.get_offset_async(_topic, _partition, start_time, 10, [this, cb](rpc_result<offset_response> response) {
        if(response)
          return cb(rpc_result<void>(response.ec));

        for(std::vector<csi::kafka::offset_response::topic_data>::const_iterator i = response->topics.begin(); i != response->topics.end(); ++i) {
          // this should always be true.
          assert(i->topic_name == _topic);
          if(i->topic_name == _topic) {
            assert(i->partitions.size() == 1); // we did only ask for one
            for(std::vector<csi::kafka::offset_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j) {
              assert(j->partition_id == _partition); // this must be ours
              // error here???? TBD
              if(j->partition_id == _partition) {
                if(j->offsets.size()) {
                  set_offset(j->offsets[0]);
                }
                cb(rpc_result<void>(rpc_error_code(response.ec.ec1, (csi::kafka::error_codes) j->error_code)));
                return;
              }
            }
          }
        }
        cb(rpc_result<void>(rpc_error_code(response.ec.ec1, csi::kafka::error_codes::Unknown))); // this should never happen
      });
    }

    rpc_result<void> lowlevel_consumer::set_offset_time(int64_t start_time) {
      std::promise<rpc_result<void>> p;
      std::future<rpc_result<void>>  f = p.get_future();
      set_offset_time_async(start_time, [&p](rpc_result<void> result) {
        p.set_value(result);
      });
      f.wait();
      return f.get();
    }

    void  lowlevel_consumer::set_offset(int64_t offset) {
      _next_offset = offset;
    }

    //void lowlevel_consumer::fetch1(fetch_callback cb) {
    //  const std::vector<partition_cursor> cursors = { { _partition, _next_offset } };
    //  _client.fetch_async(_topic, cursors, _rx_timeout, 10, _max_packet_size, [this, cb](rpc_result<fetch_response> response) {
    //    if(response) {
    //      boost::system::error_code ignored;
    //      BOOST_LOG_TRIVIAL(error) << "lowlevel_consumer::fetch() " << _client.remote_endpoint(ignored).address().to_string() << " " << _topic << ":" << _partition << " failed: " << to_string(response.ec);
    //      close("fetch() failed");
    //      cb(response.ec.ec1, response.ec.ec2, NULL);
    //    } else {
    //      for(std::vector<csi::kafka::fetch_response::topic_data>::const_iterator i = response->topics.begin(); i != response->topics.end(); ++i) {
    //        // this should always be true.
    //        if(i->topic_name == _topic) {
    //          for(std::vector<std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data>>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j) {
    //            if((*j)->partition_id == _partition)  // a partition that have been closed will not exist here so it will not be added again in the next read loop  TBD handle error here....
    //            {
    //              // there might be a better way of doing this on lower level since we know the socket rx size.... TBD
    //              for(std::vector<std::shared_ptr<basic_message>>::const_iterator k = (*j)->messages.begin(); k != (*j)->messages.end(); ++k)
    //                _metrics_total_rx_kb += ((*k)->key.size() + (*k)->value.size());
    //              _metrics_total_rx_msg += (*j)->messages.size();
    //              if((*j)->messages.size())
    //                _next_offset = (*j)->messages[(*j)->messages.size() - 1]->offset + 1;
    //              cb(response.ec.ec1, ((csi::kafka::error_codes) (*j)->error_code), *j);
    //            }
    //          }
    //        }
    //      }
    //    }
    //  });
    //}

    void lowlevel_consumer::_try_fetch() {
      //if(_rx_in_progress || !_client.is_connected() || _next_offset < 0 || !_cb || _transient_failure) when initializing form commit cursor we get -1 first time...
      if(_rx_in_progress || !_client.is_connected() || !_cb || _transient_failure || _pause_streaming)
        return;

      _rx_in_progress = true;

      const std::vector<partition_cursor> cursors = { { _partition, _next_offset } };
      _client.fetch_async(_topic, cursors, _rx_timeout, 10, _max_packet_size, [this](rpc_result<fetch_response> response) {
        if(!_cb) {
          _rx_in_progress = false;
          return;
        }

        if(response) {
          if(response.ec.ec2 == csi::kafka::OffsetOutOfRange) { // retry OffsetOutOfRange from beginning of stream... are we sure this is ALWAYS what we want???
            BOOST_LOG_TRIVIAL(warning) << "lowlevel_consumer: " << _topic << ":" << _partition << " resetting offset to earliest_available_offset";
            set_offset_time_async(csi::kafka::earliest_available_offset, [this](rpc_result<void> response) {
              if(response.ec) {
                close("set_offset_time_async failed()");
                _rx_in_progress = false;
                _cb(response.ec.ec1, response.ec.ec2, NULL);
                return;
              } else {
                BOOST_LOG_TRIVIAL(warning) << "lowlevel_consumer: " << _topic << ":" << _partition << " retrying _try_fetch()";
                _rx_in_progress = false;
                _try_fetch();
                return;
              }
            });
            return;
          } else {
            boost::system::error_code ignored;
            BOOST_LOG_TRIVIAL(error) << "lowlevel_consumer::fetch_async()" << _client.remote_endpoint(ignored).address().to_string() << " " << _topic << ":" << _partition << " fetch error: " << to_string(response.ec);
            //_transient_failure = true;
            close("fetch_async failed()");
            _cb(response.ec.ec1, response.ec.ec2, NULL);
          }
        } else {
          for(std::vector<csi::kafka::fetch_response::topic_data>::const_iterator i = response->topics.begin(); i != response->topics.end(); ++i) {
            // this should always be true.
            if(i->topic_name == _topic) {
              for(std::vector<std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data>>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j) {
                if((*j)->partition_id == _partition)  // a partition that have been closed will not exist here so it will not be added again in the next read loop  TBD handle error here....
                {
                  // there might be a better way of doiung this on lower leverl since we know the socket rx size.... TBD
                  for(std::vector<std::shared_ptr<basic_message>>::const_iterator k = (*j)->messages.begin(); k != (*j)->messages.end(); ++k)
                    _metrics_total_rx_kb += ((*k)->key.size() + (*k)->value.size());
                  _metrics_total_rx_msg += (*j)->messages.size();
                  if((*j)->messages.size())
                    _next_offset = (*j)->messages[(*j)->messages.size() - 1]->offset + 1;
                  _cb(response.ec.ec1, ((csi::kafka::error_codes) (*j)->error_code), *j);
                }
              }
            }
          }
        }
        _rx_in_progress = false;
        _try_fetch();
      });
    }

    void lowlevel_consumer::stream_async(datastream_callback cb) {
      _cb = cb;
      _ios.post([this]() {_try_fetch(); });
    }

    void lowlevel_consumer::pause(){
      _pause_streaming = true;
    }

    void lowlevel_consumer::resume() {
      _pause_streaming = false;
      _try_fetch();
    }

    void lowlevel_consumer::fetch(fetch_callback cb) {
      const std::vector<partition_cursor> cursors = { { _partition, _next_offset } };
      _client.fetch_async(_topic, cursors, _rx_timeout, 10, _max_packet_size, [this, cb](rpc_result<csi::kafka::fetch_response> response) {
        if(response) {
          if(response.ec.ec2 == csi::kafka::OffsetOutOfRange) { // retry OffsetOutOfRange from beginning of stream... are we sure this is ALWAYS what we want???
            BOOST_LOG_TRIVIAL(warning) << "lowlevel_consumer: " << _topic << ":" << _partition << " resetting offset to earliest_available_offset";
            set_offset_time_async(csi::kafka::earliest_available_offset, [this, cb](rpc_result<void> response) {
              if(response.ec) {
                rpc_result<csi::kafka::fetch_response> dummy;
                dummy.ec.ec2 = response.ec.ec2;
                cb(dummy);
                return;
              } else {
                BOOST_LOG_TRIVIAL(warning) << "lowlevel_consumer: " << _topic << ":" << _partition << " retrying fetch2()";
                fetch(cb);
              }
            });
            return;
          }
          else {
            boost::system::error_code ignored;
            BOOST_LOG_TRIVIAL(warning) << "lowlevel_consumer::fetch " << _client.remote_endpoint(ignored).address().to_string() << " " << _topic << ":" << _partition << " failed: " << to_string(response.ec);
            close("fetch failed");
            cb(response);
            return;
          }
        }

        for(std::vector<csi::kafka::fetch_response::topic_data>::const_iterator i = response->topics.begin(); i != response->topics.end(); ++i) {
          // this should always be true.
          if(i->topic_name == _topic) {
            // update cursor(s)
            for(std::vector<std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data>>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j) {
              if((*j)->partition_id == _partition)  // a partition that have been closed will not exist here so it will not be added again in the next read loop  TBD handle error here....
              {
                if((*j)->messages.size())
                  _next_offset = (*j)->messages[(*j)->messages.size() - 1]->offset + 1;
              }
            }

            //collect metrics
            for(std::vector<std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data>>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j) {
              if((*j)->partition_id == _partition)  // a partition that have been closed will not exist here so it will not be added again in the next read loop  TBD handle error here....
              {
                // there might be a better way of doing this on lower level since we know the socket rx size.... TBD
                for(std::vector<std::shared_ptr<basic_message>>::const_iterator k = (*j)->messages.begin(); k != (*j)->messages.end(); ++k)
                  _metrics_total_rx_kb += ((*k)->key.size() + (*k)->value.size());
                _metrics_total_rx_msg += (*j)->messages.size();
              }
            }
          }
        }
        cb(response);
      });
    }

    rpc_result<csi::kafka::fetch_response> lowlevel_consumer::fetch() {
      std::promise<rpc_result<csi::kafka::fetch_response>> p;
      std::future<rpc_result<csi::kafka::fetch_response>>  f = p.get_future();
      fetch([&p](rpc_result<csi::kafka::fetch_response> result) {
        p.set_value(result);
      });
      f.wait();
      return f.get();
    }

    void lowlevel_consumer::get_metadata_async(get_metadata_callback cb) {
      _client.get_metadata_async({ _topic }, cb);
    }

    rpc_result<metadata_response> lowlevel_consumer::get_metadata() {
      return _client.get_metadata({ _topic });
    }

    void lowlevel_consumer::get_group_coordinator_async(const std::string& consumer_group, get_group_coordinator_callback cb) {
      _client.get_group_coordinator_async(consumer_group, cb);
    }

    rpc_result<group_coordinator_response> lowlevel_consumer::get_group_coordinator(const std::string& consumer_group) {
      return _client.get_group_coordinator(consumer_group);
    }
  } // kafka
}; // csi
