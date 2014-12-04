#include <chrono>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>

#include <csi_kafka/avro/avro_consumer.h>
#include "syslog.h"

static boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 50000);
static int64_t total=0;



int main(int argc, char** argv)
{
    /*
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sample-avro-syslog1
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
    bin/kafka-topics.sh --list --zookeeper localhost:2181
    */

    std::string hostname = (argc >= 2) ? argv[1] : "192.168.91.135";
    std::string port = (argc >= 3) ? argv[2] : "9092";

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::avro_consumer<sample::syslog> consumer(io_service, hostname, port, "sample-avro-syslog2", 0);

    //consumer.start(csi::kafka::earliest_available_offset, [](csi::kafka::error_codes ec, const sample::syslog& log)
    consumer.start(csi::kafka::latest_offsets, [](csi::kafka::error_codes ec, const sample::syslog& log)
    {
        if (ec != 0)
        {
            std::cerr << "ec = " << ec << std::endl;
        }
        else
        {
            //boost::lexical_cast<std::string>(milliseconds());
            try
            {
                boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
                boost::posix_time::ptime log_time = boost::posix_time::from_iso_string(log.timestamp);

                //std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - log_time); return duration.count();
                auto duration = now - log_time;
                auto ms = duration.total_milliseconds();
                
                total++;
                acc((double) ms);
            }
            catch (...)
            {
            }
            //std::cerr << log.timestamp << ":" << log.message << " duration:" << duration << std::endl;
        }
    });

    while (true)
    {
        boost::this_thread::sleep(boost::posix_time::seconds(1));
        std::cerr << "roundtrip = " << boost::accumulators::rolling_mean(acc) << " (ms) total msg = " << total << std::endl;
    }

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}
