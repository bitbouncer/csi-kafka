#include <chrono>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
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

void send_batch(csi::kafka::avro_value_producer<sample::syslog>& producer)
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
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
    csi::kafka::broker_address addr("192.168.0.102", 9092);
    int32_t port = (argc >= 3) ? atoi(argv[2]) : 9092;
    if (argc >= 2)
        addr = csi::kafka::broker_address(argv[1], port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::avro_value_producer<sample::syslog> producer(io_service, "saka.test.avro-syslog2", 0);
    
    boost::system::error_code error = producer.connect(addr, 1000);

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
