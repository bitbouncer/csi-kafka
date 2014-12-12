#include <chrono>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <csi_kafka/avro/avro_producer.h>
#include "syslog.h"

static boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 10);
static int64_t total = 0;
static int lognr = 0;

void create_message(std::vector<sample::syslog>& msg)
{
    msg.clear();
    boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();

    for (int i = 0; i != 1000; ++i)
    {
        sample::syslog log;
        log.appname = "sample";
        log.hostname = "hostname";
        log.message = "thanks for the fish";
        log.msgid = lognr++;
        log.pri = 1;
        log.procid = "0";
        log.timestamp = boost::posix_time::to_iso_string(now);
        log.version = 1;
        msg.push_back(log);
    }
}

void send_batch(csi::kafka::avro_producer<sample::syslog>& producer)
{
    std::vector<sample::syslog> x;
    create_message(x);

    size_t items_in_message = x.size();
    uint32_t correlation_id = 42;
    producer.send_async(1, 1000, x, correlation_id, [items_in_message, &producer](csi::kafka::rpc_result<csi::kafka::produce_response> response)
    {
        if (response)
            std::cerr << csi::kafka::to_string(response.ec) << std::endl;
        else
            total += items_in_message;

        send_batch(producer);
    });
    std::cerr << "+";
}

int main(int argc, char** argv)
{
    /*
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sample-avro-syslog2
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
    bin/kafka-topics.sh --list --zookeeper localhost:2181
    */

    std::string hostname = (argc >= 2) ? argv[1] : "192.168.0.102";
    //std::string hostname = (argc >= 2) ? argv[1] : "z8r102-mc12-4-4.sth-tc2.videoplaza.net";
    //std::string hostname = (argc >= 2) ? argv[1] : "10.1.3.238";

    std::string port = (argc >= 3) ? argv[2] : "9092";
    boost::asio::ip::tcp::resolver::query query(hostname, port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::avro_producer<sample::syslog> producer(io_service, query, "saka.test.avro-syslog2", 0);
    
    boost::system::error_code error = producer.connect();

   send_batch(producer);

    while (true)
    {
        uint64_t last_total = total;
        boost::this_thread::sleep(boost::posix_time::seconds(1));
        acc((double)total - last_total);
        std::cerr << boost::accumulators::rolling_mean(acc) << " msg/s " << total << std::endl;
    }

    work.reset();
    io_service.stop();
    return EXIT_SUCCESS;
}
