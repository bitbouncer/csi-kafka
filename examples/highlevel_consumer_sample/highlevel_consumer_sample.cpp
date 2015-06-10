#include <boost/thread.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/highlevel_consumer.h>

int main(int argc, char** argv)
{
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
    int32_t port = (argc >= 3) ? atoi(argv[2]) : 9092;

    std::vector<csi::kafka::broker_address> brokers;
    if (argc >= 2)
    {
        brokers.push_back(csi::kafka::broker_address(argv[1], port));
    }
    else
    {
        brokers.push_back(csi::kafka::broker_address("192.168.0.108", port));
        brokers.push_back(csi::kafka::broker_address("192.168.0.109", port));
        brokers.push_back(csi::kafka::broker_address("192.168.0.110", port));
    }

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::highlevel_consumer consumer(io_service, "collectd.graphite", 500, 1000000);
      
    consumer.connect(brokers);
    //std::vector<int64_t> result = consumer.get_offsets();

    consumer.connect_forever(brokers);

    consumer.set_offset(csi::kafka::earliest_available_offset);

    boost::thread do_log([&consumer]
    {
        boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 10);
        while (true)
        {
            boost::this_thread::sleep(boost::posix_time::seconds(1));

            std::vector<csi::kafka::highlevel_consumer::metrics>  metrics = consumer.get_metrics();
            uint32_t rx_msg_sec_total = 0;
            uint32_t rx_kb_sec_total = 0;
            for (std::vector<csi::kafka::highlevel_consumer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
            {
                //std::cerr << "\t partiton:" << (*i).partition << "\t" << (*i).rx_msg_sec << " msg/s \t" << ((*i).rx_kb_sec/1024) << "MB/s \troundtrip:" << (*i).rx_roundtrip << " ms" << std::endl;
                rx_msg_sec_total += (*i).rx_msg_sec;
                rx_kb_sec_total += (*i).rx_kb_sec;
            }
            BOOST_LOG_TRIVIAL(info) << "RX: " << rx_msg_sec_total << " msg/s \t" << (rx_kb_sec_total / 1024) << "MB/s";
        }
    });


    consumer.stream_async([](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, const csi::kafka::fetch_response::topic_data::partition_data& response)
    {
        if (ec1 || ec2)
        {
            BOOST_LOG_TRIVIAL(error) << "stream failed ec1::" << ec1 << " ec2" << csi::kafka::to_string(ec2);
            return;
        }
    });

    while (true)
        boost::this_thread::sleep(boost::posix_time::seconds(30));

    consumer.close();

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}
