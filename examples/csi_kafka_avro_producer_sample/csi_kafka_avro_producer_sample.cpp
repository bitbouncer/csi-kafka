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

void write_logs()
{
    while (true)
    {
        uint64_t last_total = total;
        boost::this_thread::sleep(boost::posix_time::seconds(1));
        acc((double)total - last_total);
        std::cerr << boost::accumulators::rolling_mean(acc) << " (ms)/s " << total << std::endl;
    }
}

int main(int argc, char** argv)
{
    /*
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sample-avro-syslog1
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
    bin/kafka-topics.sh --list --zookeeper localhost:2181
    */

    //std::string hostname = (argc >= 2) ? argv[1] : "192.168.247.130";
    std::string hostname = (argc >= 2) ? argv[1] : "kafka-dev";
    std::string port = (argc >= 3) ? argv[2] : "9092";

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));
    boost::thread log_thread(boost::bind(write_logs));

    csi::kafka::avro_producer<sample::syslog> producer(io_service, hostname, port, "sample-avro-syslog1", 0);
    producer.start();


    while (true)
    {
        boost::this_thread::sleep(boost::posix_time::milliseconds(10));
        std::vector<sample::syslog> x;
        create_message(x);
        size_t items_in_message = x.size();
        producer.send_async(0, 1000, x, 0, [items_in_message](std::shared_ptr<csi::kafka::produce_response> response)
        {
            total += items_in_message;
        });
    }
 

    work.reset();
    io_service.stop();
    return EXIT_SUCCESS;
}
