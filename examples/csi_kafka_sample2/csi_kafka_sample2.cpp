#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <csi_kafka/consumer.h>
#include <csi_kafka/producer.h>
#include <csi_kafka/low_level/client.h>

int main(int argc, char** argv)
{
    std::string hostname = (argc >= 2) ? argv[1] : "192.168.91.131";
    std::string port = (argc >= 3) ? argv[2] : "9092";

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));


    csi::kafka::consumer consumer(io_service, hostname, port, "test", 0);

    consumer.start(csi::kafka::earliest_available_offset, [](const csi::kafka::fetch_response::topic_data::partition_data& data)
    {
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

    csi::kafka::producer producer(io_service, hostname, port, "test", 0);
    producer.start();

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
                std::cerr << "¨-";
            else
                std::cerr << "+";
        });
    }

    boost::this_thread::sleep(boost::posix_time::seconds(1000));

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}