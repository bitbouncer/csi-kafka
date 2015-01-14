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

    csi::kafka::highlevel_producer producer(io_service, "saka.test.ext_datastream", -1, 500, 20000);

    boost::system::error_code error = producer.connect(query);

}
