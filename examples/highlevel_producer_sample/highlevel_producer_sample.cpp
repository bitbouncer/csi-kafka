#include <boost/thread.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/high_level_producer.h>


#define VALUE_SIZE 300

void start_send(csi::kafka::highlevel_producer& producer)
{
    uint32_t key = 0;
    std::vector<std::shared_ptr<csi::kafka::basic_message>> v;
        for (int i = 0; i != 10000; ++i, ++key)
        {
            v.push_back(std::shared_ptr<csi::kafka::basic_message>(new csi::kafka::basic_message(
                std::to_string(key),
                std::string(VALUE_SIZE, 'z')
                )));
        }
        producer.send_async(v, [&producer]()
        {
            start_send(producer); // send another
        });
}


int main(int argc, char** argv)
{
    int32_t port = (argc >= 3) ? atoi(argv[2]) : 9092;

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::highlevel_producer producer(io_service, "perf-8-new", -1, 500, 20000);

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
    
    boost::thread do_log([&producer]
    {
        while (true)
        {
            boost::this_thread::sleep(boost::posix_time::seconds(1));

            std::vector<csi::kafka::highlevel_producer::metrics>  metrics = producer.get_metrics();

            size_t total_queue = 0;
            uint32_t tx_msg_sec_total = 0;
            uint32_t tx_kb_sec_total = 0;
            for (std::vector<csi::kafka::highlevel_producer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
            {
                total_queue += (*i).msg_in_queue;
                tx_msg_sec_total += (*i).tx_msg_sec;
                tx_kb_sec_total += (*i).tx_kb_sec;
            }
            std::cerr << "\t        \tqueue:" << total_queue << "\t" << tx_msg_sec_total << " msg/s \t" << tx_kb_sec_total << "KB/s" << std::endl;
        }
    });

    start_send(producer);

    while (true)
        boost::this_thread::sleep(boost::posix_time::seconds(30));

    producer.close();

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}
