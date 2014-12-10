#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <csi_kafka/low_level/consumer.h>
#include <csi_kafka/low_level/producer.h>

int main(int argc, char** argv)
{
    std::string hostname = (argc >= 2) ? argv[1] : "192.168.91.131";
    std::string port = (argc >= 3) ? argv[2] : "9092";
    boost::asio::ip::tcp::resolver::query query(hostname, port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));


    csi::kafka::lowlevel_consumer consumer(io_service, query, "saka.test.sample2");

    boost::system::error_code ec1 = consumer.connect();
    auto ec2 = consumer.set_offset(0, csi::kafka::earliest_available_offset);

    consumer.stream_async([](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, const csi::kafka::fetch_response::topic_data::partition_data& data)
    {
        if (ec1 || ec2)
        {
            std::cerr << "  fetch next failed ec1:" << ec1 << ", ec2 " << csi::kafka::to_string(ec2) << std::endl;
            return;
        }

        if (data.error_code == 0)
        {
            for (std::vector<csi::kafka::basic_message>::const_iterator i = data.messages.begin(); i != data.messages.end(); ++i)
            {
                if (i->key.is_null())
                    std::cerr << "NULL \t";
                else
                    std::cerr << (i->key.size() ? std::string((const char*)&i->key[0], i->key.size()) : "<empty>") << "\t";

                if (i->value.is_null())
                    std::cerr << "NULL" << std::endl;
                else
                    std::cerr << (i->value.size() ? std::string((const char*)&i->value[0], i->value.size()) : "<empty>") << std::endl;
            }
        }
    });


    csi::kafka::producer producer(io_service, query, "saka.test.sample2", 0);

    boost::system::error_code ec3 = producer.connect();

    std::vector<csi::kafka::basic_message> x;
    for (int i = 0; i != 1; ++i)
    {
        x.push_back(csi::kafka::basic_message("key1", "So long and thanks for all the fish"));
    }


    for (int i = 0; i != 100; ++i)
    {
        producer.send_async(1, 1000, x, 0, [](csi::kafka::error_codes error, std::shared_ptr<csi::kafka::produce_response> response)
        {
            if (error)
                std::cerr << "�-";
            else
                std::cerr << "+";
        });
    }

    boost::this_thread::sleep(boost::posix_time::seconds(1000));

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}