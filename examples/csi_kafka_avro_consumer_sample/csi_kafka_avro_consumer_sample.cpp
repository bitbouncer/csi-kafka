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
#include <csi_kafka/high_level_consumer.h>

#include <csi_kafka/avro/decoder.h>
#include "syslog.h"

int main(int argc, char** argv)
{
    std::string hostname = (argc >= 2) ? argv[1] : "192.168.0.102";
    //std::string hostname = (argc >= 2) ? argv[1] : "z8r102-mc12-4-4.sth-tc2.videoplaza.net";

    std::string port = (argc >= 3) ? argv[2] : "9092";
    boost::asio::ip::tcp::resolver::query query(hostname, port);

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));


    // just testing away
    {
        csi::kafka::low_level::client client(io_service);
        boost::system::error_code ec = client.connect(query);
        auto md = client.get_metadata({}, 0);
        auto resp = client.get_consumer_metadata("saka.test.avro-syslog2", 0);
    }

    csi::kafka::highlevel_consumer consumer0(io_service, "saka.test.avro-syslog2", 100);
    boost::system::error_code ec0 = consumer0.connect(query);


    //sample begin
    csi::kafka::lowlevel_consumer consumer(io_service, "saka.test.avro-syslog2");

    boost::system::error_code ec1 = consumer.connect(query);
    auto ec2 = consumer.set_offset(0, csi::kafka::latest_offsets);
    auto ec3 = consumer.set_offset(4, csi::kafka::latest_offsets);



    boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 50000);
    int64_t total = 0;

    consumer.stream_async(
        csi::kafka::avro_value_decoder<sample::syslog>(
        [&acc, &total](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, std::shared_ptr<sample::syslog> log)
    {
        if (ec1 || ec2)
        {
            std::cerr << " decode error: ec1:" << ec1 << " ec2" << csi::kafka::to_string(ec2) << std::endl;
            return;
        }
        if (!log)
        {
            std::cerr << " decode unexpected NULL value" << std::endl;
            return;
        }
        try
        {
            boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
            boost::posix_time::ptime log_time = boost::posix_time::from_iso_string(log->timestamp);
            auto duration = now - log_time;
            auto ms = duration.total_milliseconds();
            total++;
            acc((double)ms);
        }
        catch (...)
        {
        }
    })
        );


    while (true)
    {
        boost::this_thread::sleep(boost::posix_time::seconds(1));
        std::cerr << "roundtrip = " << boost::accumulators::rolling_mean(acc) << " (ms) total msg = " << total << std::endl;
    }

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}
