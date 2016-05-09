#include <boost/thread.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/program_options.hpp>
#include <csi_kafka/kafka.h>

#include <csi_kafka/highlevel_producer.h>
#include <csi_kafka/highlevel_consumer.h>
#include <csi_kafka/consumer_coordinator.h>


#define CONSUMER_GROUP "csi-kafka.basic_sample"
#define TOPIC_NAME     "csi-kafka.test-topic"
#define CONSUMER_ID    "csi-kafka.basic_sample_consumer_id"
#define KAFKA_BROKER   "10.1.46.13"
/*
    to get this working you have to prepare the broker
    1) first make sure the topic is existing
    2) create a path in zookeeper
    see README.txt
    */


int main(int argc, char** argv) {
  boost::program_options::options_description desc("options");
  desc.add_options()
    ("help", "produce help message")
    ("topic", boost::program_options::value<std::string>()->default_value(TOPIC_NAME), "topic")
    ("consumer_group", boost::program_options::value<std::string>()->default_value(CONSUMER_GROUP), "consumer_group")
    ("consumer_id", boost::program_options::value<std::string>()->default_value(CONSUMER_ID), "consumer_id")
    ("broker", boost::program_options::value<std::string>()->default_value(KAFKA_BROKER), "broker")
    ;

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);

  if(vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string topic;
  if(vm.count("topic")) {
    topic = vm["topic"].as<std::string>();
  } else {
    std::cout << "--topic must be specified" << std::endl;
    return 0;
  }

  std::string consumer_group;
  if(vm.count("consumer_group")) {
    consumer_group = vm["consumer_group"].as<std::string>();
  } else {
    std::cout << "--topic must be specified" << std::endl;
    return 0;
  }

  std::string consumer_id;
  if(vm.count("consumer_id")) {
    consumer_id = vm["consumer_id"].as<std::string>();
  } else {
    std::cout << "--consumer_id must be specified" << std::endl;
    return 0;
  }

  int32_t kafka_port = 9092;
  std::vector<csi::kafka::broker_address> brokers;
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

  boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::debug);

  boost::asio::io_service io_service;
  std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
  boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));


  csi::kafka::highlevel_producer producer(io_service, topic, -1, 500, 20000);
  producer.connect(brokers);

  std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message(
    "",
    "nisse"
    ));
  auto pres = producer.send_sync(0, { msg });

  csi::kafka::consumer_coordinator coordinator(io_service, consumer_group);

  auto res0 = coordinator.connect(brokers, 1000);
  if(res0) {
    std::cerr << res0.message() << std::endl;
    return -1;
  }

  auto res4 = coordinator.get_consumer_offset(topic);
  if(res4) {
    std::cerr << to_string(res4.ec) << std::endl;
    return -1;
  }

  int32_t ec;
  auto offsets = parse(res4, topic, ec);
  if(ec) {
    std::cerr << to_string((csi::kafka::error_codes) ec) << std::endl;
    return -1;
  }

  // resert the offsets
  for(std::vector<csi::kafka::topic_offset>::iterator i = offsets.begin(); i != offsets.end(); ++i)
    i->offset = csi::kafka::earliest_available_offset;

  auto res5 = coordinator.commit_consumer_offset(-1, "my test prog", topic, offsets, "graff");

  if(res5) {
    std::cerr << to_string(res5.ec) << std::endl;
    return -1;
  }


  //lots of errors can happen anyway here...
  for(std::vector<csi::kafka::offset_commit_response::topic_data>::const_iterator i = res5->topics.begin(); i != res5->topics.end(); ++i) {
    assert(i->topic_name == topic);
    for(std::vector<csi::kafka::offset_commit_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j) {
      if(j->error_code) {
        std::cerr << "partition " << j->partition_id << to_string((csi::kafka::error_codes) j->error_code) << std::endl;
      } else {
        std::cerr << "partition " << j->partition_id << " OK " << std::endl;
      }
    }
  }

  auto res6 = coordinator.get_consumer_offset(topic);
  if(res6) {
    std::cerr << to_string(res6.ec) << std::endl;
    return -1;
  }

  work.reset();
  io_service.stop();

  return EXIT_SUCCESS;
}
