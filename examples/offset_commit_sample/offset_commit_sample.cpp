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


int main(int argc, char** argv)
{
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

    if (vm.count("help"))
    {
        std::cout << desc << std::endl;
        return 0;
    }

    std::string topic;
    if (vm.count("topic"))
    {
        topic = vm["topic"].as<std::string>();
    }
    else
    {
        std::cout << "--topic must be specified" << std::endl;
        return 0;
    }

    std::string consumer_group;
    if (vm.count("consumer_group"))
    {
        consumer_group = vm["consumer_group"].as<std::string>();
    }
    else
    {
        std::cout << "--topic must be specified" << std::endl;
        return 0;
    }

    std::string consumer_id;
    if (vm.count("consumer_id"))
    {
        consumer_id = vm["consumer_id"].as<std::string>();
    }
    else
    {
        std::cout << "--consumer_id must be specified" << std::endl;
        return 0;
    }

    int32_t kafka_port = 9092;
    std::vector<csi::kafka::broker_address> brokers;
    if (vm.count("broker"))
    {
        std::string s = vm["broker"].as<std::string>();
        size_t last_colon = s.find_last_of(':');
        if (last_colon != std::string::npos)
            kafka_port = atoi(s.substr(last_colon + 1).c_str());
        s = s.substr(0, last_colon);

        // now find the brokers...
        size_t last_separator = s.find_last_of(',');
        while (last_separator != std::string::npos)
        {
            std::string host = s.substr(last_separator + 1);
            brokers.push_back(csi::kafka::broker_address(host, kafka_port));
            s = s.substr(0, last_separator);
            last_separator = s.find_last_of(',');
        }
        brokers.push_back(csi::kafka::broker_address(s, kafka_port));
    }
    else
    {
        std::cout << "--broker must be specified" << std::endl;
        return 0;
    }

    std::cout << "broker(s)      : ";
    for (std::vector<csi::kafka::broker_address>::const_iterator i = brokers.begin(); i != brokers.end(); ++i)
    {
        std::cout << i->host_name << ":" << i->port;
        if (i != brokers.end() - 1)
            std::cout << ", ";
    }
    std::cout << std::endl;
    std::cout << "topic          : " << topic << std::endl;

    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));


    csi::kafka::highlevel_producer producer(io_service, topic, -1, 500, 20000);
    producer.connect(brokers);

    std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message(
       "",
        "nisse"
        ));
    msg->partition = 0;
    auto pres = producer.send_sync({ msg });
    



    csi::kafka::consumer_coordinator coordinator(io_service, topic, consumer_group, 1000);

    auto res0 = coordinator.connect(brokers[0], 3000); // errror should be a list...
    if (res0)
    {
        std::cerr << res0.message() << std::endl;
        return -1;
    }

    auto res1 = coordinator.get_group_coordinator();
    if (res1)
    {
        std::cerr << to_string(res1.ec) << std::endl;
        return -1;
    }

    // now we need to connect to the right host - should be moved inside coordinator....
    coordinator.close();
    csi::kafka::broker_address coordinator_address(res1.data->coordinator_host, res1.data->coordinator_port);
    auto res3 = coordinator.connect(coordinator_address, 2000);
    if (res3)
    {
        std::cerr << res3.message() << std::endl;
        return -1;
    }


    if (res1->error_code)
    {
        std::cerr << to_string(csi::kafka::error_codes(res1->error_code)) << std::endl;
        return -1;
    }


    auto res4 = coordinator.get_consumer_offset(0);
    if (res4)
    {
        std::cerr << to_string(res4.ec) << std::endl;
        return -1;
    }

    //int32_t consumer_group_generation_id, const std::string& consumer_id, int32_t partition, int64_t offset, const std::string& metadata)
    auto res5 = coordinator.commit_consumer_offset(-1, "my test prog", 0, -1, "graff");
    if (res5)
    {
        std::cerr << to_string(res5.ec) << std::endl;
        return -1;
    }

    //lots of errors can happen anyway here...
    for (auto& i = res5->topics.begin(); i != res5->topics.end(); ++i)
    {
        assert(i->topic_name == topic);
        for (auto& j = i->partitions.begin(); j != i->partitions.end(); ++j)
        {
            if (j->error_code)
            {
                std::cerr << "partition " << j->partition_id << to_string((csi::kafka::error_codes) j->error_code) << std::endl;
            }
            else
            {
                std::cerr << "partition " << j->partition_id << " OK " << std::endl;
            }
        }
    }

    auto res6 = coordinator.get_consumer_offset(0);
    if (res6)
    {
        std::cerr << to_string(res6.ec) << std::endl;
        return -1;
    }

    /*
    auto res2 = client.connect(csi::kafka::broker_address(res1.data->host_name, res1.data->port), 3000);
    if (res2)
    {
        std::cerr << res2.message() << std::endl;
        return -1;
    }

    auto res3 = client.get_consumer_offset(CONSUMER_GROUP, TOPIC_NAME, 0, 44);
    if (res3)
    {
        std::cerr << to_string(res3.ec) << std::endl;
        return -1;
    }

    auto res4 = client.commit_consumer_offset(CONSUMER_GROUP, 1, CONSUMER_ID, TOPIC_NAME, 0, 22, "nisse", 44);
    if (res4)
    {
        std::cerr << to_string(res4.ec) << std::endl;
        return -1;
    }
    client.close();
    */


    ////HIGHLEVEL CONSUMER
    //csi::kafka::highlevel_consumer consumer(io_service, TOPIC_NAME, 1000, 10000);
    //consumer.connect( { broker } );
    ////std::vector<int64_t> result = consumer.get_offsets();
    //consumer.connect_forever({ broker } );

    //consumer.set_offset(csi::kafka::earliest_available_offset);

    //boost::thread do_log([&consumer]
    //{
    //    boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 10);
    //    while (true)
    //    {
    //        boost::this_thread::sleep(boost::posix_time::seconds(1));

    //        std::vector<csi::kafka::highlevel_consumer::metrics>  metrics = consumer.get_metrics();
    //        uint32_t rx_msg_sec_total = 0;
    //        uint32_t rx_kb_sec_total = 0;
    //        for (std::vector<csi::kafka::highlevel_consumer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
    //        {
    //            std::cerr << "\t partiton:" << (*i).partition << "\t" << (*i).rx_msg_sec << " msg/s \t" << (*i).rx_kb_sec << "KB/s \troundtrip:" << (*i).rx_roundtrip << " ms" << std::endl;
    //            rx_msg_sec_total += (*i).rx_msg_sec;
    //            rx_kb_sec_total += (*i).rx_kb_sec;
    //        }
    //        std::cerr << "\t          \t" << rx_msg_sec_total << " msg/s \t" << rx_kb_sec_total << "KB/s" << std::endl;
    //    }
    //});

 

    while (true)
        boost::this_thread::sleep(boost::posix_time::seconds(30));

    //consumer.close();

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}
