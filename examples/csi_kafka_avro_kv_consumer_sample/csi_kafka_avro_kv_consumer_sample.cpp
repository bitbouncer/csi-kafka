#include <chrono>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>

#include <csi_kafka/low_level/consumer.h>
#include <csi_kafka/low_level/utility.h>
#include <csi_kafka/avro/decoder.h>

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
    csi::kafka::broker_address addr("192.168.0.6", 9092);
    int32_t port = (argc >= 3) ? atoi(argv[2]) : 9092;
    if (argc >= 2)
        addr = csi::kafka::broker_address(argv[1], port);

    csi::kafka::table<sample::contact_info_key, sample::contact_info, contact_info_key_compare> datastore;

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::lowlevel_consumer consumer(io_service, "saka.test.avro_key_value", 0, 1000);

    int64_t message_total = 0;

    boost::system::error_code ec1 = consumer.connect(addr, 1000);
    auto ec2 = consumer.set_offset(csi::kafka::earliest_available_offset);

    csi::kafka::avro_key_value_decoder<sample::contact_info_key, sample::contact_info> decoder([&datastore, &message_total](
        const boost::system::error_code& ec1, 
        csi::kafka::error_codes ec2, 
        std::shared_ptr<sample::contact_info_key> key, 
        std::shared_ptr<sample::contact_info> value,
        int partition,
        int64_t offset)
    {
        if (ec1 || ec2)
        {
            std::cerr << "ec1 = " << ec1 << " ec2 " << ec2 << std::endl;
            return;
        }
        if (key)
            datastore.put(*key, value);
        message_total++;
    });

    consumer.stream_async(decoder);

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
