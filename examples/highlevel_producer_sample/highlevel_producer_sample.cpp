#include <boost/thread.hpp>

#include <csi_kafka/kafka.h>
#include <csi_kafka/high_level_producer.h>

int main(int argc, char** argv)
{
    std::stringstream stream;
    std::string hostname = (argc >= 2) ? argv[1] : "192.168.0.102";
    //std::string hostname = (argc >= 2) ? argv[1] : "z8r102-mc12-4-4.sth-tc2.videoplaza.net";
    //std::string hostname = (argc >= 2) ? argv[1] : "kafka-dev";
    std::string port = (argc >= 3) ? argv[2] : "9092";

    boost::asio::ip::tcp::resolver::query query(hostname, port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::highlevel_producer producer(io_service, query, "test");

    boost::system::error_code error = producer.connect();
    
    std::vector<csi::kafka::basic_message> x;
    for (int i = 0; i != 1; ++i)
    {
        x.push_back(csi::kafka::basic_message("key1", "So long and thanks for all the fish"));
    }

    /*
    auto res7 = producer.send_produce("test", 0, 0, 1000, x, 0);
    if (res7)
        std::cerr << csi::kafka::to_string(res7.ec) << std::endl;
    */

    boost::this_thread::sleep(boost::posix_time::seconds(30));

    producer.close();

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}
