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
    

    uint32_t key = 0;
    while (true)
    {
        for (int i = 0; i != 10000; ++i, ++key)
        {
            producer.enqueue(std::shared_ptr<csi::kafka::basic_message>(new csi::kafka::basic_message(std::to_string(key), "So long and thanks for all the fish")));
        }
        boost::this_thread::sleep(boost::posix_time::milliseconds(100));
    }

    boost::this_thread::sleep(boost::posix_time::seconds(30));

    producer.close();

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}
