#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <csi_kafka/lowlevel_consumer.h>
#include <csi_kafka/lowlevel_producer.h>

int main(int argc, char** argv)
{
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
    std::string hostname = (argc >= 2) ? argv[1] : "192.168.0.102";
    std::string port = (argc >= 3) ? argv[2] : "9092";
    boost::asio::ip::tcp::resolver::query query(hostname, port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::lowlevel_client client(io_service);
    boost::system::error_code ec = client.connect(query, 1000);
    if (!ec)
    {
        auto md = client.get_metadata({}, 0);

        auto cco = client.commit_consumer_offset("my_test_group", 1, "offset_checker", "saka.test.avro-syslog2", 0, 17, "gnarf", 17);
        auto cmd = client.get_consumer_metadata("my_test_group", 42);

        if (!cmd)
        {
            boost::asio::ip::tcp::resolver::query query(cmd->host_name, std::to_string(cmd->port));
            csi::kafka::lowlevel_client offset_client(io_service);
            boost::system::error_code ec = offset_client.connect(query, 1000);
            if (!ec)
            {
                auto od = offset_client.get_consumer_offset("my_test_group", 42);
            }
        }

        auto of = client.get_metadata({}, 0);
    }

    boost::this_thread::sleep(boost::posix_time::seconds(1000));

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}