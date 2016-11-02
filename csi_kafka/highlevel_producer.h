#include <csi_kafka/lowlevel_producer.h>
#include <csi_kafka/internal/async_metadata_client.h>

#pragma once

namespace csi {
  namespace kafka {

    class highlevel_producer {
    public:
      struct metrics {
        int         partition;
        std::string host;
        int         port;
        size_t      msg_in_queue;
        size_t      bytes_in_queue;
        uint32_t    tx_kb_sec;
        uint32_t    tx_msg_sec;
        double      tx_roundtrip;
      };

      typedef std::function <void(const boost::system::error_code&)> connect_callback;
      typedef std::function <void(int32_t ec)>                       tx_ack_callback;

      highlevel_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t required_acks, int32_t tx_timeout, int32_t max_packet_size = -1);
      ~highlevel_producer();
      void                      close();
      boost::asio::io_service&  io_service() { return _ios; }
      void                      connect_forever(const std::vector<broker_address>& brokers); // , connect_callback cb);  // stream of connection events??
      void                      connect_async(const std::vector<broker_address>& brokers, connect_callback cb);
      boost::system::error_code connect(const std::vector<broker_address>& brokers);
      void                      send_async(std::shared_ptr<csi::kafka::basic_message> message, tx_ack_callback = NULL);
      void                      send_async(std::vector<std::shared_ptr<csi::kafka::basic_message>>& messages, tx_ack_callback = NULL);
      int32_t                   send_sync(std::shared_ptr<csi::kafka::basic_message> message);
      int32_t                   send_sync(std::vector<std::shared_ptr<csi::kafka::basic_message>>& messages);
      size_t                    items_in_queue() const;
      std::vector<metrics>      get_metrics() const;
      inline const std::string& topic() const { return _topic; }
      inline size_t             partitions() const { return _partition2partitions.size(); }
    private:
      struct tx_item {
        tx_item(std::shared_ptr<csi::kafka::basic_message> message) : msg(message) {}
        tx_item(std::shared_ptr<csi::kafka::basic_message> message, tx_ack_callback callback) : msg(message), cb(callback) {}
        std::shared_ptr<csi::kafka::basic_message> msg;
        tx_ack_callback                            cb;
      };

      // asio callbacks
      void handle_timer(const boost::system::error_code& ec);
      void handle_response(rpc_result<metadata_response> result);
      void _try_connect_brokers();
      boost::asio::io_service&                                                 _ios;
      const std::string                                                        _topic;
      int32_t                                                                  _required_acks;
      int32_t                                                                  _tx_timeout;
      int32_t                                                                  _max_packet_size;
      boost::asio::deadline_timer			                                         _timer;
      boost::posix_time::time_duration	                                       _timeout;
      // CLUSTER METADATA
      csi::kafka::async_metadata_client                                        _meta_client;
      mutable csi::spinlock                                                    _spinlock; // protects the metadata below
      std::map<int, broker_data>                                               _broker2brokers;
      std::map<int, csi::kafka::metadata_response::topic_data::partition_data> _partition2partitions;
      std::deque<tx_item>                                                      _tx_queue; // used when waiting for cluster
      std::map<int, lowlevel_producer*>                                        _partition2producers;
    };
  };
};
