#include <boost/thread.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/high_level_consumer.h>


#define VALUE_SIZE 800

static boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 10);
static int64_t total = 0;
static int lognr = 0;

int main(int argc, char** argv)
{
    //std::stringstream stream;
    std::string hostname = (argc >= 2) ? argv[1] : "192.168.0.106";
    //std::string hostname = (argc >= 2) ? argv[1] : "z8r102-mc12-4-4.sth-tc2.videoplaza.net";
    std::string port = (argc >= 3) ? argv[2] : "9092";

    boost::asio::ip::tcp::resolver::query query(hostname, port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::highlevel_consumer consumer(io_service, "saka.test.ext_datastream", 20000);

    boost::system::error_code error = consumer.connect(query);

    boost::thread do_log([&consumer]
    {
        boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 10);
        while (true)
        {
            uint64_t last_total = total;
            boost::this_thread::sleep(boost::posix_time::seconds(1));
            acc((double)total - last_total);
            std::cerr << boost::accumulators::rolling_mean(acc) << " (ms)/s " << total << std::endl; // varför gör vi detta???

            std::vector<csi::kafka::highlevel_consumer::metrics>  metrics = consumer.get_metrics();

            uint32_t rx_msg_sec_total = 0;
            uint32_t rx_kb_sec_total = 0;
            for (std::vector<csi::kafka::highlevel_consumer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
            {
                std::cerr << "\t partiton:" << (*i).partition << "\t" << (*i).rx_msg_sec << " msg/s \t" << (*i).rx_kb_sec << "KB/s \troundtrip:" << (*i).rx_roundtrip << " ms" << std::endl;
                rx_msg_sec_total += (*i).rx_msg_sec;
                rx_kb_sec_total += (*i).rx_kb_sec;
            }
            std::cerr << "\t          \t" << rx_msg_sec_total << " msg/s \t" << rx_kb_sec_total << "KB/s" << std::endl;
        }
    });


    consumer.stream_async([](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, const csi::kafka::fetch_response::topic_data::partition_data& response)
    {
        if (ec1 || ec2)
        {
            std::cerr << " decode error: ec1:" << ec1 << " ec2" << csi::kafka::to_string(ec2) << std::endl;
            return;
        }
        std::cerr << "+";
    });

    while (true)
        boost::this_thread::sleep(boost::posix_time::seconds(30));

    consumer.close();

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}
