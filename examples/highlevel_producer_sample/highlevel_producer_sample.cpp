#include <thread>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/highlevel_producer.h>


#define VALUE_SIZE 300

void start_send(csi::kafka::highlevel_producer& producer) {
  uint32_t key = 0;
  std::vector<std::shared_ptr<csi::kafka::basic_message>> v;
  for(int i = 0; i != 10000; ++i, ++key) {
    v.push_back(std::shared_ptr<csi::kafka::basic_message>(new csi::kafka::basic_message(
      std::to_string(key),
      std::string(VALUE_SIZE, 'z')
      )));
  }
  producer.send_async(v, [&producer](int32_t ec) {
    if (!ec)
      start_send(producer); // send another
  });
}

int main(int argc, char** argv) {
  boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
  int32_t port = (argc >= 3) ? atoi(argv[2]) : 9092;

  boost::asio::io_service io_service;
  std::unique_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
  std::thread run_thread([&io_service] { io_service.run(); });

  csi::kafka::highlevel_producer producer(io_service, "perf-8-new", -1, 500, 20000);

  std::vector<csi::kafka::broker_address> brokers;
  if(argc >= 2) {
    brokers.push_back(csi::kafka::broker_address(argv[1], port));
  } else {
    brokers.push_back(csi::kafka::broker_address("192.168.0.108", port));
    brokers.push_back(csi::kafka::broker_address("192.168.0.109", port));
    brokers.push_back(csi::kafka::broker_address("192.168.0.110", port));
  }

  while (producer.connect(brokers, 1000)) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    BOOST_LOG_TRIVIAL(info) << "retrying to connect";
  } 

  //producer.connect_forever(brokers); this is not working anymore - we have to add something like a connection lost event callback and let the user reconnect...

  std::thread do_log([&producer] {

    uint64_t last_tx_msg_total = 0;
    uint64_t last_tx_bytes_total = 0;

    while (true)
    {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      std::vector<csi::kafka::highlevel_producer::metrics>  metrics = producer.get_metrics();

      size_t total_queue = 0;
      uint64_t tx_msg_total = 0;
      uint64_t tx_bytes_total = 0;
      for (std::vector<csi::kafka::highlevel_producer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
      {
        total_queue += (*i).msg_in_queue;
        tx_msg_total += (*i).total_tx_msg;
        tx_bytes_total += (*i).total_tx_bytes;
      }

      uint64_t msg_per_sec = (tx_msg_total - last_tx_msg_total);
      uint64_t bytes_per_sec = (tx_bytes_total - last_tx_bytes_total);

      last_tx_msg_total = tx_msg_total;
      last_tx_bytes_total = tx_bytes_total;

      if (msg_per_sec)
      {
        BOOST_LOG_TRIVIAL(info) << "kafka: topic: " << producer.topic() << ", tx queue:" << total_queue << ", tx " << msg_per_sec << " msg/sec, (" << (bytes_per_sec / (1024 * 1024)) << " MB/s)";
      }
    }
  });

  start_send(producer);

  while(true)
    std::this_thread::sleep_for(std::chrono::seconds(30));

  producer.close();

  work.reset();
  io_service.stop();

  return EXIT_SUCCESS;
}
