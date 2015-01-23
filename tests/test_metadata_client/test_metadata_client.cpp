#include <boost/thread.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/internal/async_metadata_client.h>

int main(int argc, char** argv)
{
    int32_t port = (argc >= 3) ? atoi(argv[2]) : 9092;
   
    if (port <= 0)
    {
        std::cerr << "usage argv[0] << host [port]" << std::endl;
        std::cerr << "     default port=9092" << std::endl;
    }

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::async_metadata_client client(io_service);


    auto res0 = client.get_metadata({ "saka.test.ext_datastream" }, 42);

    if (argc >= 2)
    {
        client.connect_async({ csi::kafka::broker_address(argv[1], port) }, NULL);
    }
    else
    {
        client.connect_async(
        {
            csi::kafka::broker_address("192.168.0.6", 9092),
            csi::kafka::broker_address("z8r102-mc12-4-4.sth-tc2.videoplaza.net", 9092),
            //csi::kafka::broker_address("10.1.3.238", 9092)
        }, NULL);
    }

    while (!client.is_connected())
    {
        boost::this_thread::sleep(boost::posix_time::seconds(1));
    }

    
    //auto res1 = client.get_metadata({ "saka.test.ext_datastream" }, 42);
    auto res2 = client.get_metadata({ "saka.test.int_datastream" }, 42);
    //auto res3 = client.get_metadata({ "dedushka1" }, 42);

    while (true)
    {
        auto res2 = client.get_metadata({ "saka.test.int_datastream" }, 42);
        if (!res2)
        {
            for (auto i = res2->topics.begin(); i != res2->topics.end(); i++)
            {
                for (auto j = i->partitions.begin(); j != i->partitions.end(); j++)
                {
                    std::cerr << i->topic_name << " Partition: " << j->partition_id << " Leader: " << j->leader << " Replicas: ";
                    for (auto k = j->replicas.begin(); k != j->replicas.end(); k++)
                        std::cerr << *k << ", ";
                    std::cerr << "Isr: ";
                    for (auto k = j->isr.begin(); k != j->isr.end(); k++)
                        std::cerr << *k << ", ";
                    std::cerr << std::endl;
                }

            }
        }
        boost::this_thread::sleep(boost::posix_time::seconds(10));
    }
    
    client.close();
    io_service.stop();
    bt.join();
}
