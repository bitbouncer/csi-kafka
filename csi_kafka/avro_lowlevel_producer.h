#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <csi_kafka/lowlevel_producer.h>

#pragma once

namespace csi
{
    namespace kafka
    {
        template<class T>
        void avro_binary_encode(const T& src, avro::OutputStream& dst)
        {
            avro::EncoderPtr e = avro::binaryEncoder();
            e->init(dst);
            avro::encode(*e, src);
            // push back unused characters to the output stream again... really strange... 			
            // otherwise content_length will be a multiple of 4096
            e->flush();
        }

        template<class T>
        class avro_value_producer : public csi::kafka::lowlevel_producer
        {
        public:
            avro_value_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition) :
                lowlevel_producer(io_service, topic, partition)
            {
            }

            void send_async(int32_t required_acks, int32_t timeout, const std::vector<T>& src, int32_t correlation_id, send_callback cb)
            {
                std::vector<std::shared_ptr<csi::kafka::basic_message>> src2;
                for (typename std::vector<T>::const_iterator i = src.begin(); i != src.end(); ++i)
                {
                    auto ostr = avro::memoryOutputStream();
                    avro_binary_encode(*i, *ostr);
                    size_t sz = ostr->byteCount();

                    auto in = avro::memoryInputStream(*ostr);
                    avro::StreamReader stream_reader(*in);

                    std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message());
                    
                    msg->value.set_null(false);
                    msg->value.reserve(sz + 16); // add 128 bit hash before - 0's for now
                    //msg->value.insert(msg.value.end(), 0, 16);
                    for (int j = 0; j != sz; ++j)
                        msg->value.push_back(stream_reader.read());
                    src2.push_back(msg);
                }
                csi::kafka::lowlevel_producer::send_async(required_acks, timeout, src2, correlation_id, cb);
            }
        };

        template<class K, class V>
        class avro_key_value_producer : public csi::kafka::lowlevel_producer
        {
        public:
            avro_key_value_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t partition) :
                lowlevel_producer(io_service, topic, partition)
            {
            }

            void send_async(int32_t required_acks, int32_t timeout, const std::vector<std::pair<K, V>>& src, int32_t correlation_id, send_callback cb)
            {
                std::vector<std::shared_ptr<csi::kafka::basic_message>> src2;
                for (typename std::vector<std::pair<K, V>>::const_iterator i = src.begin(); i != src.end(); ++i)
                {
                    std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message());

                    //encode key
                    {
                        auto ostr = avro::memoryOutputStream();
                        avro_binary_encode(i->first, *ostr);
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
                        avro_binary_encode(i->second, *ostr);
                        size_t sz = ostr->byteCount();

                        auto in = avro::memoryInputStream(*ostr);
                        avro::StreamReader stream_reader(*in);
                        msg->value.set_null(false);
                        msg->value.reserve(sz);
                        for (int j = 0; j != sz; ++j)
                            msg->value.push_back(stream_reader.read());
                    }
                    src2.push_back(msg);
                }
                csi::kafka::lowlevel_producer::send_async(required_acks, timeout, src2, correlation_id, cb);
            }
        };
    }
}