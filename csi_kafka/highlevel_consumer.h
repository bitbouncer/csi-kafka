#include <map>
#include <csi_kafka/lowlevel_consumer.h>
#include <csi_kafka/internal/async_metadata_client.h>

#pragma once

namespace csi {
  namespace kafka {
    class highlevel_consumer {
    public:
      struct metrics {
        int         partition;
        std::string host;
        int         port;
        uint32_t    rx_kb_sec;
        uint32_t    rx_msg_sec;
        double      rx_roundtrip;
      };

      typedef boost::function <void(const boost::system::error_code&)> connect_callback;
      typedef boost::function <void(const boost::system::error_code& ec1, csi::kafka::error_codes ec2, std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data>)> datastream_callback;

      typedef boost::function <void(std::vector<rpc_result<csi::kafka::fetch_response>>)> fetch_callback;
      typedef boost::function <void(rpc_result<group_coordinator_response>)>              get_group_coordinator_callback;

      enum { MAX_FETCH_SIZE = basic_call_context::MAX_BUFFER_SIZE };

      highlevel_consumer(boost::asio::io_service& io_service, const std::string& topic, int32_t rx_timeout, size_t max_packet_size = MAX_FETCH_SIZE);
      highlevel_consumer(boost::asio::io_service& io_service, const std::string& topic, const std::vector<int>& partion_mask, int32_t rx_timeout, size_t max_packet_size = MAX_FETCH_SIZE);
      ~highlevel_consumer();

      void                                                connect_forever(const std::vector<broker_address>& brokers); // , connect_callback cb);  // stream of connection events??
      void                                                connect_async(const std::vector<broker_address>& brokers, connect_callback cb);
      boost::system::error_code                           connect(const std::vector<broker_address>& brokers);
      void                                                set_offset(int64_t start_time);
      void                                                set_offset(const std::vector<topic_offset>& offsets);
      void                                                set_offset(const std::map<int32_t, int64_t>&);
      std::map<int32_t, int64_t>                          get_next_offset() const;
      void                                                close();

      void                                                stream_async(datastream_callback cb);
      void                                                pause();
      void                                                resume();

      void                                                fetch(fetch_callback cb);
      std::vector<rpc_result<csi::kafka::fetch_response>> fetch();
      std::vector<metrics>                                get_metrics() const;
      inline const std::string&                           topic() const { return _topic; }

    private:
      void handle_response(rpc_result<metadata_response> result);
      void handle_timer(const boost::system::error_code& ec);
      void _connect_async(connect_callback cb);
      void _try_connect_brokers();
      boost::asio::io_service&                                                    _ios;
      boost::asio::deadline_timer			                                            _timer;
      boost::posix_time::time_duration	                                          _timeout;
      const std::string                                                           _topic;
      int32_t                                                                     _rx_timeout;
      size_t                                                                      _max_packet_size;
      std::map<int, lowlevel_consumer*>                                           _partition2consumers;

      // CLUSTER METADATA
      csi::kafka::async_metadata_client                                           _meta_client;
      mutable csi::spinlock                                                       _spinlock; // protects the metadata below
      std::vector<int>                                                            _partitions_mask;
      std::map<int, broker_data>                                                  _broker2brokers;
      std::map<int, csi::kafka::metadata_response::topic_data::partition_data>    _partition2partitions; // partition->partition_dat
      // CONSUMER METADATA
      csi::kafka::async_metadata_client                                           _consumer_meta_client;
    };
  };
};
