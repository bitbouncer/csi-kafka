#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <csi_kafka/high_level_producer.h>

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
    

        template<class K, class V>
        class avro_high_level_producer : public csi::kafka::highlevel_producer
        {
        public:
            avro_high_level_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t required_acks, int32_t tx_timeout, int32_t max_packet_size = -1) :
                highlevel_producer(io_service, topic, required_acks, tx_timeout, max_packet_size)
            {
            }

            void send_async(const std::vector<std::pair<K, V>>& src, tx_ack_callback cb)
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
                csi::kafka::highlevel_producer::send_async(src2, cb);
            }
        };
    }
}