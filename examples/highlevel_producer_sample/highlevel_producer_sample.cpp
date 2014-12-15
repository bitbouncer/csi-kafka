#include <boost/thread.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/high_level_producer.h>


#define VALUE_SIZE 800

static boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 10);
static int64_t total = 0;
static int lognr = 0;

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
        total += v.size();
}


int main(int argc, char** argv)
{
    std::stringstream stream;
    //std::string hostname = (argc >= 2) ? argv[1] : "192.168.0.102";
    std::string hostname = (argc >= 2) ? argv[1] : "z8r102-mc12-4-4.sth-tc2.videoplaza.net";
    std::string port = (argc >= 3) ? argv[2] : "9092";

    boost::asio::ip::tcp::resolver::query query(hostname, port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::highlevel_producer producer(io_service, "saka.test.strings", -1, 500, 20000);

    boost::system::error_code error = producer.connect(query);
    
    boost::thread do_log([&producer]
    {
        boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 10);
        while (true)
        {
            uint64_t last_total = total;
            boost::this_thread::sleep(boost::posix_time::seconds(1));
            acc((double)total - last_total);
            std::cerr << boost::accumulators::rolling_mean(acc) << " (ms)/s " << total << std::endl;

            std::vector<csi::kafka::highlevel_producer::metrics>  metrics = producer.get_metrics();

            size_t total_queue = 0;
            uint32_t tx_msg_sec_total = 0;
            uint32_t tx_kb_sec_total = 0;
            for (std::vector<csi::kafka::highlevel_producer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
            {
                std::cerr << "\t partiton:" << (*i).partition << "\tqueue:" << (*i).msg_in_queue << "\t" << (*i).tx_msg_sec << " msg/s \t" << (*i).tx_kb_sec << "KB/s \troundtrip:" << (*i).tx_roundtrip  << " ms" << std::endl;
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
