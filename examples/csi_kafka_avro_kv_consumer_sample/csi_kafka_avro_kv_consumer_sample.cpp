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
#include <csi_kafka/low_level/utility.h>

#include "contact_info.h"
#include "contact_info_key.h"

struct contact_info_key_compare
{
    bool operator() (const sample::contact_info_key& lhs, const sample::contact_info_key& rhs)
    {
        return lhs.md5 < rhs.md5;
    }
};

int main(int argc, char** argv)
{
    csi::kafka::table<sample::contact_info_key, sample::contact_info, contact_info_key_compare> datastore;

    std::string hostname = (argc >= 2) ? argv[1] : "192.168.91.131";
    std::string port = (argc >= 3) ? argv[2] : "9092";

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::avro_consumer2<sample::contact_info_key, sample::contact_info> consumer(io_service, hostname, port, "sample_avro_key_value", 0);
    int64_t message_total = 0;
    consumer.start(csi::kafka::earliest_available_offset, [&datastore, &message_total](csi::kafka::error_codes ec, std::shared_ptr<sample::contact_info_key> key, std::shared_ptr<sample::contact_info> value)
    {
        if (ec != 0)
        {
            std::cerr << "ec = " << ec << std::endl;
            return;
        }

        if (key)
            datastore.put(*key, value);

        message_total++;
    });

    static boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 3);
    while (true)
    {
        uint64_t last_total = message_total;
        boost::this_thread::sleep(boost::posix_time::seconds(1));
        acc((double)message_total - last_total);
        std::cerr << boost::accumulators::rolling_mean(acc) << " (ms)/s dbsize = " << datastore.size() << " total messages " << message_total << std::endl;
    }

    work.reset();
    io_service.stop();
    return EXIT_SUCCESS;
}
