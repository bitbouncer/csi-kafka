#include <boost/thread.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/high_level_producer.h>

#define VALUE_SIZE 800

// NOT WORKING - JUST STARTED....
int main(int argc, char** argv)
{
    int32_t port = (argc >= 3) ? atoi(argv[2]) : 9092;

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::highlevel_producer producer(io_service, "saka.test.ext_datastream", -1, 500, 20000);

    if (argc >= 2)
    {
        producer.connect_async({ csi::kafka::broker_address(argv[1], port) });
    }
    else
    {
        producer.connect_async(
        {
            csi::kafka::broker_address("192.168.0.6", 9092),
            csi::kafka::broker_address("10.1.3.238", 9092)
        });
    }
}
