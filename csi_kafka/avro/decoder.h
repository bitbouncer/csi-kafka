#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>

#include <csi_kafka/kafka.h>

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

        template<class V>
        class avro_value_decoder
        {
        public:
            typedef boost::function <void(csi::kafka::error_codes, std::shared_ptr<V> value)> avro_callback;
            avro_value_decoder(avro_callback cb) : _cb(cb) {}

            void operator()(csi::kafka::error_codes ec, const csi::kafka::fetch_response::topic_data::partition_data& data)
            {
                if (ec)
                {
                    _cb((csi::kafka::error_codes) ec, std::shared_ptr<V>(NULL));
                    return;
                }
                else if (data.error_code != 0)
                {
                    _cb((csi::kafka::error_codes) data.error_code, std::shared_ptr<V>(NULL));
                    return;
                }

                for (std::vector<csi::kafka::basic_message>::const_iterator i = data.messages.begin(); i != data.messages.end(); ++i)
                {
                    if (!i->value.is_null())
                    {
                        // decode avro in value...
                        std::shared_ptr<V> value = std::shared_ptr<V>(new V());
                        std::auto_ptr<avro::InputStream> src = avro::memoryInputStream(&i->value[0], i->value.size()); // lets always reserve 128 bits for md5 hash of avro schema so it's possible to dynamically decode things
                        avro_binary_decode(src, *value);
                        _cb((csi::kafka::error_codes) data.error_code, value);
                    }
                    else
                    {
                        _cb((csi::kafka::error_codes) data.error_code, std::shared_ptr<V>(NULL)); // can this happen???
                    }
                }
            }
        private:
            avro_callback _cb;
        };

        template<class K, class V>
        class avro_key_value_decoder
        {
        public:
            typedef boost::function <void(csi::kafka::error_codes, std::shared_ptr<K> key, std::shared_ptr<V> value)> avro_callback;

            avro_key_value_decoder(avro_callback cb) : _cb(cb) {}

            void operator()(csi::kafka::error_codes ec, const csi::kafka::fetch_response::topic_data::partition_data& data)
            {
                if (ec != 0)
                {
                    _cb((csi::kafka::error_codes) ec, std::shared_ptr<K>(), std::shared_ptr<V>());
                    return;
                }
                else if (data.error_code != 0)
                {
                    _cb((csi::kafka::error_codes) data.error_code, std::shared_ptr<K>(), std::shared_ptr<V>());
                    return;
                }

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

                    _cb((csi::kafka::error_codes) data.error_code, key, value);
                }
            }

        protected:
            avro_callback _cb;
        };
    }
}