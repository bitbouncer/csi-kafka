#include <boost/thread.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/lowlevel_client.h>
#include <csi_kafka/encoder.h>

int main(int argc, char** argv)
{
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
    std::stringstream stream;
    //std::string hostname = (argc >= 2) ? argv[1] : "192.168.91.131";
    std::string hostname = (argc >= 2) ? argv[1] : "10.100.5.53";
    //std::string hostname = (argc >= 2) ? argv[1] : "kafka-dev";
    std::string port = (argc >= 3) ? argv[2] : "9092";

    boost::asio::ip::tcp::resolver::query query(hostname, port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::lowlevel_client client(io_service);

    boost::system::error_code error = client.connect(query, 1000);

    auto res1 = client.get_metadata({}, 0);
    if (res1)
        std::cerr << csi::kafka::to_string(res1.ec) << std::endl;

    auto res2 = client.get_offset("test", 0, csi::kafka::earliest_available_offset, 1000, 0);
    if (res2)
        std::cerr << csi::kafka::to_string(res2.ec) << std::endl;

    auto res3 = client.get_consumer_offset("my_test_consumer", 0);
    if (res3)
        std::cerr << csi::kafka::to_string(res3.ec) << std::endl;

    auto res4 = client.commit_consumer_offset("my_test_consumer", "test", 0, 0, 0, "my metadata", 0);
    if (res4)
        std::cerr << csi::kafka::to_string(res4.ec) << std::endl;

    auto res5 = client.get_consumer_offset("my_test_consumer", "test", 2, 0);
    if (res5)
        std::cerr << csi::kafka::to_string(res5.ec) << std::endl;

    csi::kafka::partition_cursor cursor(0, csi::kafka::earliest_available_offset);
    auto res6 = client.get_data("test", { cursor }, 1000, 1000, 0);
    if (res6)
        std::cerr << csi::kafka::to_string(res6.ec) << std::endl;

    std::vector<std::shared_ptr<csi::kafka::basic_message>> x;
    for (int i = 0; i != 1; ++i)
    {
        x.push_back(std::shared_ptr<csi::kafka::basic_message>(new csi::kafka::basic_message("key1", "So long and thanks for all the fish")));
    }

    auto res7 = client.send_produce("test", 0, 0, 1000, x, 0);
    if (res7)
        std::cerr << csi::kafka::to_string(res7.ec) << std::endl;

    client.close();

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}
