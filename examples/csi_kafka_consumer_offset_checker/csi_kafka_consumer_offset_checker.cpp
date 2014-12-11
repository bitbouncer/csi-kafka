#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <csi_kafka/low_level/consumer.h>
#include <csi_kafka/low_level/producer.h>

int main(int argc, char** argv)
{
    std::string hostname = (argc >= 2) ? argv[1] : "192.168.0.102";
    //std::string hostname = (argc >= 2) ? argv[1] : "z8r102-mc12-4-4.sth-tc2.videoplaza.net";

    std::string port = (argc >= 3) ? argv[2] : "9092";
    boost::asio::ip::tcp::resolver::query query(hostname, port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::low_level::client client(io_service, query);
    boost::system::error_code ec = client.connect();
    auto md  = client.get_metadata({}, 0);
    auto cmd = client.get_consumer_metadata("saka.test.avro-syslog2", 0);

    boost::this_thread::sleep(boost::posix_time::seconds(1000));

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}