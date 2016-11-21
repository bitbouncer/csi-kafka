#include <boost/thread.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/program_options.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/highlevel_consumer.h>

int main(int argc, char** argv) {
  int32_t kafka_port = 9092;
  std::string topic;
  std::vector<csi::kafka::broker_address> brokers;

  boost::program_options::options_description desc("options");
  desc.add_options()
    ("help", "produce help message")
    ("topic", boost::program_options::value<std::string>(), "topic")
    ("broker", boost::program_options::value<std::string>(), "broker")
    ;

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);

  if(vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  if(vm.count("topic")) {
    topic = vm["topic"].as<std::string>();
  } else {
    std::cout << "--topic must be specified" << std::endl;
    return 0;
  }

  if(vm.count("broker")) {
    std::string s = vm["broker"].as<std::string>();
    size_t last_colon = s.find_last_of(':');
    if(last_colon != std::string::npos)
      kafka_port = atoi(s.substr(last_colon + 1).c_str());
    s = s.substr(0, last_colon);

    // now find the brokers...
    size_t last_separator = s.find_last_of(',');
    while(last_separator != std::string::npos) {
      std::string host = s.substr(last_separator + 1);
      brokers.push_back(csi::kafka::broker_address(host, kafka_port));
      s = s.substr(0, last_separator);
      last_separator = s.find_last_of(',');
    }
    brokers.push_back(csi::kafka::broker_address(s, kafka_port));
  } else {
    std::cout << "--broker must be specified" << std::endl;
    return 0;
  }

  std::cout << "broker(s)      : ";
  for(std::vector<csi::kafka::broker_address>::const_iterator i = brokers.begin(); i != brokers.end(); ++i) {
    std::cout << i->host_name << ":" << i->port;
    if(i != brokers.end() - 1)
      std::cout << ", ";
  }
  std::cout << std::endl;
  std::cout << "topic          : " << topic << std::endl;


  boost::asio::io_service io_service;
  std::unique_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
  boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

  csi::kafka::highlevel_consumer consumer(io_service, topic, 500, 1000000);

  consumer.connect(brokers);
  consumer.connect_forever(brokers);
  consumer.set_offset(csi::kafka::earliest_available_offset);

  boost::thread do_log([&consumer] {
    uint64_t last_rx_msg_total = 0;
    uint64_t last_rx_bytes_total = 0;

    while (true)
    {
      boost::this_thread::sleep(boost::posix_time::seconds(1));

      std::vector<csi::kafka::highlevel_consumer::metrics>  metrics = consumer.get_metrics();

      uint32_t rx_msg_total = 0;
      uint32_t rx_bytes_total = 0;
      for (std::vector<csi::kafka::highlevel_consumer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
      {
        rx_msg_total += (*i).total_rx_msg;
        rx_bytes_total += (*i).total_rx_bytes;
      }

      uint64_t msg_per_sec = (rx_msg_total - last_rx_msg_total);
      uint64_t bytes_per_sec = (rx_bytes_total - last_rx_bytes_total);

      last_rx_msg_total = rx_msg_total;
      last_rx_bytes_total = rx_bytes_total;

      if (msg_per_sec)
      {
        BOOST_LOG_TRIVIAL(info) << "kafka: topic: " << consumer.topic() << ", rx " << msg_per_sec << " msg/sec, (" << (bytes_per_sec / (1024 * 1024)) << " MB/s)";
      }
    }
  });


  consumer.stream_async([](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data> response) {
    if(ec1 || ec2) {
      BOOST_LOG_TRIVIAL(error) << "stream failed ec1::" << ec1 << " ec2" << csi::kafka::to_string(ec2);
      return;
    }
  });

  while(true)
    boost::this_thread::sleep(boost::posix_time::seconds(30));

  consumer.close();

  work.reset();
  io_service.stop();

  return EXIT_SUCCESS;
}
