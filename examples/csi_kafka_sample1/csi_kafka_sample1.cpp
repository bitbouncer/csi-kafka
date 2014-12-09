#include <boost/thread.hpp>

#include <csi_kafka/kafka.h>
#include <csi_kafka/low_level/client.h>
#include <csi_kafka/low_level/encoder.h>

int main(int argc, char** argv)
{
    std::stringstream stream;
    std::string hostname = (argc >= 2) ? argv[1] : "192.168.91.131";
    //std::string hostname = (argc >= 2) ? argv[1] : "kafka-dev";
    std::string port = (argc >= 3) ? argv[2] : "9092";

    boost::asio::ip::tcp::resolver::query query(hostname, port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::low_level::client client(io_service, query);
    
    boost::system::error_code error = client.connect();

    client.perform_sync(csi::kafka::create_metadata_request({}, 0), [](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
    {
        if (!ec)
        {
            auto response = csi::kafka::parse_metadata_response(handle);
        }
        std::cerr << "callback error_code=" << ec << std::endl;
    });

    boost::this_thread::sleep(boost::posix_time::seconds(1));

    client.perform_async(csi::kafka::create_simple_offset_request("test", 0, csi::kafka::earliest_available_offset, 1000, 0), [](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
    {
        if (!ec)
        {
            auto response = csi::kafka::parse_offset_response(handle);
        }
        std::cerr << "callback error_code=" << ec << std::endl;
    });

    boost::this_thread::sleep(boost::posix_time::seconds(1));


    client.perform_async(csi::kafka::create_consumer_metadata_request("my_test_consumer", 0), [](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
    {
        if (!ec)
        {
            auto response = csi::kafka::parse_consumer_metadata_response(handle);
            std::cerr << "kafka error code " << response->error_code << std::endl;
        }
        std::cerr << "callback error_code=" << ec << std::endl;
    });

    boost::this_thread::sleep(boost::posix_time::seconds(1));

    client.perform_async(csi::kafka::create_simple_offset_commit_request("my_test_consumer", "test", 0, 0, 0, "my metadata", 0), [](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
    {
        if (!ec)
        {
            auto response = csi::kafka::parse_offset_commit_response(handle);
        }
        std::cerr << "callback error_code=" << ec << std::endl;
    });

    client.perform_async(csi::kafka::create_simple_offset_fetch_request("my_test_consumer", "test", 2, 2), [](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
    {
        if (!ec)
        {
            auto response = csi::kafka::parse_offset_fetch_response(handle);
        }
        std::cerr << "callback error_code=" << ec << std::endl;
    });

    client.perform_async(csi::kafka::create_simple_fetch_request("test", 0, 0, 1000, 1000, 0), [](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
    {
        if (!ec)
        {
            auto response = csi::kafka::parse_fetch_response(handle);
        }
        std::cerr << "callback error_code=" << ec << std::endl;
    });

    boost::this_thread::sleep(boost::posix_time::seconds(1));

    std::vector<csi::kafka::basic_message> x;
    for (int i = 0; i != 1; ++i)
    {
        x.push_back(csi::kafka::basic_message("key1", "So long and thanks for all the fish"));
    }

    for (int i = 0; i != 0; ++i)
    {
        client.perform_async(csi::kafka::create_produce_request("test", 0, 0, 1000, x, 0), [](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
        {
            std::cerr << "callback error_code=" << ec << std::endl;
        });
    }

    boost::this_thread::sleep(boost::posix_time::seconds(1000));

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}
