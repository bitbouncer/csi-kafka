#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <csi_avro/encoding.h>
#include <csi_kafka/async_lowlevel_producer.h>

#pragma once

namespace csi
{
    namespace kafka
    {
        template<class T>
        class avro_value_producer : public csi::kafka::async_lowlevel_producer
        {
        public:
            avro_value_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition, int32_t required_acks, int32_t timeout, int32_t max_packet_size) :
                async_lowlevel_producer(io_service, topic, partition, required_acks, timeout, max_packet_size)
            {
            }

            void send_async(const std::vector<T>& src, int32_t correlation_id, tx_ack_callback cb)
            {
                for (typename std::vector<T>::const_iterator i = src.begin(); i != src.end(); ++i)
                {
                    auto ostr = avro::memoryOutputStream();
                    avro_binary_encode_with_schema(*i, *ostr);
                    size_t sz = ostr->byteCount();

                    auto in = avro::memoryInputStream(*ostr);
                    avro::StreamReader stream_reader(*in);

                    std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message());
                    
                    msg->value.set_null(false);
                    msg->value.reserve(sz + 16); // add 128 bit hash before - 0's for now
                    //msg->value.insert(msg.value.end(), 0, 16);
                    for (int j = 0; j != sz; ++j)
                        msg->value.push_back(stream_reader.read());
                    
                    if (i != (src.end()-1))
                        csi::kafka::async_lowlevel_producer::send_async(msg, correlation_id, NULL);
                    else
                        csi::kafka::async_lowlevel_producer::send_async(msg, correlation_id, cb);
                }
            }
        };

        template<class K, class V>
        class avro_key_value_producer : public csi::kafka::async_lowlevel_producer
        {
        public:
            avro_key_value_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition, int32_t required_acks, int32_t timeout, int32_t max_packet_size) :
                async_lowlevel_producer(io_service, topic, partition, required_acks, timeout, max_packet_size)
            {
            }

            void send_async(const std::vector<std::pair<K, V>>& src, int32_t correlation_id, tx_ack_callback cb)
            {
                for (typename std::vector<std::pair<K, V>>::const_iterator i = src.begin(); i != src.end(); ++i)
                {
                    std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message());

                    //encode key
                    {
                        auto ostr = avro::memoryOutputStream();
                        avro_binary_encode_with_fingerprint(i->first, *ostr);
                        size_t sz = ostr->byteCount();

                        auto in = avro::memoryInputStream(*ostr);
                        avro::StreamReader stream_reader(*in);
                        msg->key.set_null(false);
                        msg->key.reserve(sz);
                        for (int j = 0; j != sz; ++j)
                            msg->key.push_back(stream_reader.read());
                    }

                    //encode value
                    {
                        auto ostr = avro::memoryOutputStream();
                        avro_binary_encode_with_fingerprint(i->second, *ostr);
                        size_t sz = ostr->byteCount();

                        auto in = avro::memoryInputStream(*ostr);
                        avro::StreamReader stream_reader(*in);
                        msg->value.set_null(false);
                        msg->value.reserve(sz);
                        for (int j = 0; j != sz; ++j)
                            msg->value.push_back(stream_reader.read());
                    }
                    if (i != (src.end() - 1))
                        csi::kafka::async_lowlevel_producer::send_async(msg, NULL);
                    else
                        csi::kafka::async_lowlevel_producer::send_async(msg, cb);
                }
            }
        };
    }
}