#include <chrono>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <csi_kafka/highlevel_consumer.h>
#include <csi_kafka/internal/utility.h>
#include <csi_avro_utils/confluent_codec.h>

int main(int argc, char** argv)
{
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::debug);

	int32_t kafka_port = (argc >= 3) ? atoi(argv[2]) : 9092;
	std::vector<csi::kafka::broker_address> brokers;
	if (argc >= 2)
	{
		brokers.push_back(csi::kafka::broker_address(argv[1], kafka_port));
	}
	else
	{
		brokers.push_back(csi::kafka::broker_address("192.168.0.108", kafka_port));
		brokers.push_back(csi::kafka::broker_address("192.168.0.109", kafka_port));
		brokers.push_back(csi::kafka::broker_address("192.168.0.110", kafka_port));
	}

	boost::asio::io_service fg_ios;
	std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(fg_ios));
	boost::thread fg(boost::bind(&boost::asio::io_service::run, &fg_ios));

	csi::kafka::highlevel_consumer consumer(fg_ios, "csi.dev.avro_highlevel_producer_sample", 20, 10000);
	confluent::registry            registry(fg_ios, "192.168.0.108:8081");
	confluent::codec               avro_codec(registry);

    int64_t message_total = 0;

	boost::system::error_code ec1 = consumer.connect(brokers);
	consumer.connect_forever(brokers);

    consumer.set_offset(csi::kafka::earliest_available_offset);

	boost::thread do_log([&consumer]
	{
		while (true)
		{
			boost::this_thread::sleep(boost::posix_time::seconds(1));

			std::vector<csi::kafka::highlevel_consumer::metrics>  metrics = consumer.get_metrics();
			uint32_t rx_msg_sec_total = 0;
			uint32_t rx_kb_sec_total = 0;
			for (std::vector<csi::kafka::highlevel_consumer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
			{
				rx_msg_sec_total += (*i).rx_msg_sec;
				rx_kb_sec_total += (*i).rx_kb_sec;
			}
			std::cerr << "\t\t" << rx_msg_sec_total << " msg/s \t" << (rx_kb_sec_total / 1024) << "MB/s" << std::endl;
		}
	});

	// generic decoding
	while (true)
	{
		auto r = consumer.fetch();
		for (std::vector<csi::kafka::highlevel_consumer::fetch_response>::const_iterator i = r.begin(); i != r.end(); ++i)
		{
			if ((*i).data)
			{
				const std::vector<std::shared_ptr<csi::kafka::basic_message>>& messages((*i).data->messages);
				for (std::vector<std::shared_ptr<csi::kafka::basic_message>>::const_iterator j = messages.begin(); j != messages.end(); ++j)
				{
					// decode key
					if (!(*j)->key.is_null())
					{
						std::auto_ptr<avro::InputStream> stream = avro::memoryInputStream(&(*j)->key[0], (*j)->key.size());
						auto res = avro_codec.decode_datum(&*stream);
						// do something with key...
					}

					//decode value
					if (!(*j)->value.is_null())
					{
						std::auto_ptr<avro::InputStream> stream = avro::memoryInputStream(&(*j)->value[0], (*j)->value.size());
						auto res = avro_codec.decode_datum(&*stream);
					}
				} // message
			} // has data
		} // per connection
	}

	work.reset();
	fg_ios.stop();
    return EXIT_SUCCESS;
}
