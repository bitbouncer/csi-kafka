#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <csi_kafka/producer.h>

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
        class avro_producer : public csi::kafka::producer
        {
        public:
            avro_producer(boost::asio::io_service& io_service, const std::string& hostname, const std::string& port, const std::string& topic, int32_t partition) :
                producer(io_service, hostname, port, topic, partition)
            {
            }

            void send_async(int32_t required_acks, int32_t timeout, const std::vector<T>& src, int32_t correlation_id, callback cb)
            {
                std::vector<csi::kafka::basic_message> src2;
                for (std::vector<T>::const_iterator i = src.begin(); i != src.end(); ++i)
                {
                    auto ostr = avro::memoryOutputStream();
                    avro_binary_encode(*i, *ostr);
                    size_t sz = ostr->byteCount();

                    auto in = avro::memoryInputStream(*ostr);
                    avro::StreamReader stream_reader(*in);

                    csi::kafka::basic_message msg;
                    
                    msg.value.set_null(false);
                    msg.value.reserve(sz + 16); // add 128 bit hash before - 0's for now
                    //msg.value.insert(msg.value.end(), 0, 16);
                    for (int j = 0; j != sz; ++j)
                        msg.value.push_back(stream_reader.read());
                    src2.push_back(msg);
                }
                csi::kafka::producer::send_async(required_acks, timeout, src2, correlation_id, cb);
            }

            std::shared_ptr<csi::kafka::produce_response> send_sync(int32_t required_acks, int32_t timeout, const std::vector<T>& src, int32_t correlation_id, callback cb)
            {
                std::vector<csi::kafka::basic_message> src2;
                for (std::vector<T>::const_iterator i = src.begin(); i != src.end(); ++i)
                {
                    auto ostr = avro::memoryOutputStream();
                    avro_binary_encode(*i, *ostr);
                    size_t sz = ostr->byteCount();

                    auto in = avro::memoryInputStream(*ostr);
                    avro::StreamReader stream_reader(*in);

                    csi::kafka::basic_message msg;

                    msg.value.set_null(false);
                    msg.value.reserve(sz + 16); // add 128 bit hash before - 0's for now
                    //msg.value.insert(msg.value.end(), 0, 16);
                    for (int j = 0; j != sz; ++j)
                        msg.value.push_back(stream_reader.read());
                    src2.push_back(msg);
                }
                return csi::kafka::producer::send_sync(required_acks, timeout, src2, correlation_id, cb);
            }

        };

        template<class K, class V>
        class avro_producer2 : public csi::kafka::producer
        {
        public:
            avro_producer2(boost::asio::io_service& io_service, const std::string& hostname, const std::string& port, const std::string& topic, int32_t partition) :
                producer(io_service, hostname, port, topic, partition)
            {
            }

            void send_async(int32_t required_acks, int32_t timeout, const std::vector<std::pair<K, V>>& src, int32_t correlation_id, callback cb)
            {
                std::vector<csi::kafka::basic_message> src2;
                for (std::vector<std::pair<K, V>>::const_iterator i = src.begin(); i != src.end(); ++i)
                {
                    csi::kafka::basic_message msg;

                    //encode key
                    {
                        auto ostr = avro::memoryOutputStream();
                        avro_binary_encode(i->first, *ostr);
                        size_t sz = ostr->byteCount();

                        auto in = avro::memoryInputStream(*ostr);
                        avro::StreamReader stream_reader(*in);
                        msg.key.set_null(false);
                        msg.key.reserve(sz);
                        for (int j = 0; j != sz; ++j)
                            msg.key.push_back(stream_reader.read());
                    }

                    //encode value
                    {
                        auto ostr = avro::memoryOutputStream();
                        avro_binary_encode(i->second, *ostr);
                        size_t sz = ostr->byteCount();

                        auto in = avro::memoryInputStream(*ostr);
                        avro::StreamReader stream_reader(*in);
                        msg.value.set_null(false);
                        msg.value.reserve(sz);
                        for (int j = 0; j != sz; ++j)
                            msg.value.push_back(stream_reader.read());
                    }
                    src2.push_back(msg);
                }
                csi::kafka::producer::send_async(required_acks, timeout, src2, correlation_id, cb);
            }

            std::shared_ptr<csi::kafka::produce_response> send_sync(int32_t required_acks, int32_t timeout, const std::vector<std::pair<K, V>>& src, int32_t correlation_id, callback cb)
            {
                std::vector<csi::kafka::basic_message> src2;
                for (std::vector<std::pair<K, V>>::const_iterator i = src.begin(); i != src.end(); ++i)
                {
                    csi::kafka::basic_message msg;

                    //encode key
                    {
                        auto ostr = avro::memoryOutputStream();
                        avro_binary_encode(i->first, *ostr);
                        size_t sz = ostr->byteCount();

                        auto in = avro::memoryInputStream(*ostr);
                        avro::StreamReader stream_reader(*in);
                        msg.key.set_null(false);
                        msg.key.reserve(sz);
                        for (int j = 0; j != sz; ++j)
                            msg.key.push_back(stream_reader.read());
                    }

                    //encode value
                    {
                        auto ostr = avro::memoryOutputStream();
                        avro_binary_encode(i->second, *ostr);
                        size_t sz = ostr->byteCount();

                        auto in = avro::memoryInputStream(*ostr);
                        avro::StreamReader stream_reader(*in);
                        msg.value.set_null(false);
                        msg.value.reserve(sz);
                        for (int j = 0; j != sz; ++j)
                            msg.value.push_back(stream_reader.read());
                    }
                    src2.push_back(msg);
                }
                return csi::kafka::producer::send_sync(required_acks, timeout, src2, correlation_id, cb);
            }
        };
    }
}