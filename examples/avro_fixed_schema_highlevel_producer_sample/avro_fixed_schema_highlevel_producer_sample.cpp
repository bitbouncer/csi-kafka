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
#include <csi_kafka/highlevel_producer.h>
#include <csi_avro_utils/confluent_codec.h>
#include "contact_info.h"
#include "contact_info_key.h"

#include <csi_avro_utils/avro_schema_id_encoding.h>

void create_message(std::vector<std::pair<sample::contact_info_key, sample::contact_info>>& v, int32_t& cursor)
{
    v.clear();
    for (int i = 0; i != 10000; ++i, ++cursor)
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

void encode_messages(const std::vector<std::pair<sample::contact_info_key, sample::contact_info>>& src, int32_t key_id, int32_t value_id, std::vector<std::shared_ptr<csi::kafka::basic_message>>& dst)
{
	dst.reserve(src.size());
	for (std::vector<std::pair<sample::contact_info_key, sample::contact_info>>::const_iterator i = src.begin(); i != src.end(); ++i)
	{
		std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message());

		//encode key
		{
			auto ostr = avro::memoryOutputStream();
			csi::avro_binary_encode_with_schema_id(key_id, i->first, *ostr);
			size_t sz = ostr->byteCount();

			auto in = avro::memoryInputStream(*ostr);
			avro::StreamReader stream_reader(*in);
			msg->key.set_null(false);
			msg->key.resize(sz);
			stream_reader.readBytes(msg->key.data(), sz);
		}

		//encode value
		{
			auto ostr = avro::memoryOutputStream();
			csi::avro_binary_encode_with_schema_id(value_id, i->second, *ostr);
			size_t sz = ostr->byteCount();

			auto in = avro::memoryInputStream(*ostr);
			avro::StreamReader stream_reader(*in);
			msg->value.set_null(false);
			msg->value.resize(sz);
			stream_reader.readBytes(msg->value.data(), sz);
		}
		dst.push_back(msg);
	}
}


void send_messages(int32_t key_id, int32_t val_id, csi::kafka::highlevel_producer& producer, int id)
{
	int32_t cursor = 0;
	std::vector<std::pair<sample::contact_info_key, sample::contact_info>> v;
	create_message(v, cursor);

	while (true)
	{
		std::vector<std::shared_ptr<csi::kafka::basic_message>> v2;
		encode_messages(v, key_id, val_id, v2);
		int32_t ec = producer.send_sync(v2);
		std::cerr << id;
		if (cursor > 10000000)
			cursor = 0;
	}
}

int main(int argc, char** argv)
{
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
    int64_t total = 0;
    
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

	csi::kafka::highlevel_producer producer(fg_ios, "csi.dev.avro_highlevel_producer_sample", -1, 200, 1000000);
	confluent::registry            registry(fg_ios, "192.168.0.108:8081");
	confluent::codec               avro_codec(registry);

	producer.connect(brokers);
	std::cerr << "connected to kafka" << std::endl;
	producer.connect_forever(brokers);

	boost::thread do_log([&producer]
	{
		while (true)
		{
			boost::this_thread::sleep(boost::posix_time::seconds(1));

			std::vector<csi::kafka::highlevel_producer::metrics>  metrics = producer.get_metrics();

			size_t total_queue = 0;
			uint32_t tx_msg_sec_total = 0;
			uint32_t tx_kb_sec_total = 0;
			for (std::vector<csi::kafka::highlevel_producer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
			{
				total_queue += (*i).msg_in_queue;
				tx_msg_sec_total += (*i).tx_msg_sec;
				tx_kb_sec_total += (*i).tx_kb_sec;
			}
			std::cerr << "\t        \tqueue:" << total_queue << "\t" << tx_msg_sec_total << " msg/s \t" << (tx_kb_sec_total / 1024) << "MB/s" << std::endl;
		}
	});


	std::cerr << "registring schemas" << std::endl;
	auto key_res = avro_codec.put_schema("sample.contact_info_key", sample::contact_info_key::valid_schema());

	if (key_res.first!=0)
	{
		std::cerr << "registring sample.contact_info_key failed" << std::endl;
		return -1;
	}
	auto val_res = avro_codec.put_schema("sample.contact_info", sample::contact_info::valid_schema());
	if (val_res.first!=0)
	{
		std::cerr << "registring sample.contact_info failed" << std::endl;
		return -1;
	}
	std::cerr << "registring schemas done" << std::endl;
	
    //produce messages

	std::vector<boost::thread*> threads;
	
	for (int i = 0; i != 10; ++i)
	{
		threads.emplace_back(new boost::thread([key_res, val_res, &producer, i]
		{
			send_messages(key_res.second, val_res.second, producer, i);
		}));
	}

	while (true)
	{
		boost::this_thread::sleep(boost::posix_time::seconds(1));
	}


    work.reset();
	fg_ios.stop();
    return EXIT_SUCCESS;
}
