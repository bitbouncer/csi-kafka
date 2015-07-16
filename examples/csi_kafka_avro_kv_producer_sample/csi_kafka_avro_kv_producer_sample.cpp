#include <chrono>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <csi_kafka/avro_lowlevel_producer.h>
#include "contact_info.h"
#include "contact_info_key.h"

void create_message(std::vector<std::pair<sample::contact_info_key, sample::contact_info>>& v, int32_t& cursor)
{
    v.clear();
    for (int i = 0; i != 1000; ++i, ++cursor)
    {
        std::string email = std::string("nisse") + boost::lexical_cast<std::string>(cursor)+"@csi.se";

        sample::contact_info_key key;
        key.md5 = email; // ugly but works as md5 for now is just a string in the json

        sample::contact_info value;
        value.care_of.set_null();
        value.city.set_string("stockholm");
        value.country.set_string("sweden");
        value.date_of_birth.set_null();
        value.email.set_string(email);
        value.family_name.set_string("gul");
        value.given_name.set_string(std::string("nisse") + boost::lexical_cast<std::string>(cursor));
        value.nin.set_null();
        value.postal_code.set_string("111 11");
        value.street_address.set_string("street 7a");
        v.push_back(std::pair<sample::contact_info_key, sample::contact_info>(key, value));
    }
}

int main(int argc, char** argv)
{
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
    int64_t total = 0;
    
    csi::kafka::broker_address addr("192.168.0.102", 9092);
    int32_t port = (argc >= 3) ? atoi(argv[2]) : 9092;
    if (argc >= 2)
        addr = csi::kafka::broker_address(argv[1], port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    //print out statistics
    boost::thread do_log([&total]
    {
        boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 10);
        while (true)
        {
            uint64_t last_total = total;
            boost::this_thread::sleep(boost::posix_time::seconds(1));
            acc((double)total - last_total);
            std::cerr << boost::accumulators::rolling_mean(acc) << " (ms)/s " << total << std::endl;
        }
    });

    csi::kafka::avro_key_value_producer<sample::contact_info_key, sample::contact_info> producer(io_service, "saka.test.avro_key_value", 0, -1, 500, 10000);
    boost::system::error_code error = producer.connect(addr, 1000);

    int32_t cursor=0;


    //produce messages
    while (true)
    {
        std::vector<std::pair<sample::contact_info_key, sample::contact_info>> v;
        create_message(v, cursor);
        producer.send_async(v, 0, [](int32_t ec)
        {
            std::cerr << "+";
        });
        total += v.size();
        if (cursor > 10000000)
            cursor = 0;
        boost::this_thread::sleep(boost::posix_time::milliseconds(100));
    }


    work.reset();
    io_service.stop();
    return EXIT_SUCCESS;
}
