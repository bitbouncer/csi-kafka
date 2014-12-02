#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>

#include <csi_kafka/consumer.h>

#pragma once
namespace csi
{
    namespace kafka
    {
        template<class T>
        T& avro_binary_decode(std::auto_ptr<avro::InputStream> src, T& dst)
        {
            avro::DecoderPtr e = avro::binaryDecoder();
            e->init(*src);
            avro::decode(*e, dst);
            return dst;
        }

        template<class T>
        class avro_consumer : public csi::kafka::consumer
        {
        public:
            typedef boost::function <void(csi::kafka::error_codes, const T&)> avro_callback;

            avro_consumer(boost::asio::io_service& io_service, const std::string& hostname, const std::string& port, const std::string& topic, int32_t partition) :
                consumer(io_service, hostname, port, topic, partition)
            {}

            void start(int64_t start_time, avro_callback cb)
            {
                _avro_cb = cb;
                csi::kafka::consumer::start(start_time, [this](const csi::kafka::fetch_response::topic_data::partition_data& data)
                {
                    if (data.error_code != 0)
                    {
                        _avro_cb((csi::kafka::error_codes) data.error_code, T());
                    }
                    else
                    {
                        for (std::vector<csi::kafka::basic_message>::const_iterator i = data.messages.begin(); i != data.messages.end(); ++i)
                        {
                            if (!i->value.is_null())
                            {
                                // decode avro in value...
                                T dst;
                                //std::auto_ptr<avro::InputStream> src = avro::memoryInputStream(&i->value[16], i->value.size() - 16); // lets always reserve 128 bits for md5 hash of avro schema so it's possible to dynamically decode things
                                std::auto_ptr<avro::InputStream> src = avro::memoryInputStream(&i->value[0], i->value.size()); // lets always reserve 128 bits for md5 hash of avro schema so it's possible to dynamically decode things
                                avro_binary_decode(src, dst);
                                _avro_cb((csi::kafka::error_codes) data.error_code, dst);
                            }
                        }
                    }
                });
            }

        protected:
            avro_callback _avro_cb;
        };


        template<class K, class V>
        class avro_consumer2 : public csi::kafka::consumer
        {
        public:
            typedef boost::function <void(csi::kafka::error_codes, std::shared_ptr<K> key, std::shared_ptr<V> value)> avro_callback;

            avro_consumer2(boost::asio::io_service& io_service, const std::string& hostname, const std::string& port, const std::string& topic, int32_t partition) :
                consumer(io_service, hostname, port, topic, partition)
            {}

            void start(int64_t start_time, avro_callback cb)
            {
                _avro_cb = cb;
                csi::kafka::consumer::start(start_time, [this](const csi::kafka::fetch_response::topic_data::partition_data& data)
                {
                    if (data.error_code != 0)
                    {
                        _avro_cb((csi::kafka::error_codes) data.error_code, std::shared_ptr<K>(), std::shared_ptr<V>());
                    }
                    else
                    {
                        for (std::vector<csi::kafka::basic_message>::const_iterator i = data.messages.begin(); i != data.messages.end(); ++i)
                        {
                            std::shared_ptr<K> key;
                            std::shared_ptr<V> value;

                            // decode key                            
                            if (!i->key.is_null())
                            {
                                key = std::shared_ptr<K>(new K());
                                std::auto_ptr<avro::InputStream> src = avro::memoryInputStream(&i->key[0], i->key.size()); // lets always reserve 128 bits for md5 hash of avro schema so it's possible to dynamically decode things
                                avro_binary_decode(src, *key);
                            }

                            //decode value
                            if (!i->value.is_null())
                            {
                                value = std::shared_ptr<V>(new V());
                                std::auto_ptr<avro::InputStream> src = avro::memoryInputStream(&i->value[0], i->value.size()); // lets always reserve 128 bits for md5 hash of avro schema so it's possible to dynamically decode things
                                avro_binary_decode(src, *value);
                            }

                            _avro_cb((csi::kafka::error_codes) data.error_code, key, value);
                        }
                    }
                });
            }

        protected:
            avro_callback _avro_cb;
        };
    }
}