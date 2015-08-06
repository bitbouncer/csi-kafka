#include <iostream>
#include <iomanip>
#include <string>
#include <assert.h>
#include <thread>
#include <chrono>
#include <sstream>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/asio.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/bind.hpp>
#include <boost/uuid/uuid.hpp>
#include <avro/Compiler.hh>
#include <csi_http/server/http_server.h>
#include <csi_http/csi_http.h>
#include <csi_http/encoding/avro_json_encoding.h>
#include <csi_avro_utils//utils.h>

#include <csi_kafka/highlevel_consumer.h>
#include <csi_kafka/highlevel_producer.h>
#include <csi_kafka/internal/utility.h>
#include "avro/schema_registry_post_subjects_request_t.h"
#include "avro/schema_registry_post_subjects_response_t.h"
#include "avro/schema_registry_get_subjects_response_t.h"

class schema_registry
{
public:
    schema_registry(boost::asio::io_service& ios, const std::string& topic_name) :
        _consumer(ios, topic_name, 1000, 20000),
        _producer(ios, topic_name, -1, 500, 20000)
    {
    }

    ~schema_registry() {}

    void connect(const std::vector<csi::kafka::broker_address>& brokers)
    {
        _producer.connect(brokers);
        _consumer.connect(brokers);
        _consumer.set_offset(csi::kafka::earliest_available_offset);
        _consumer.stream_async([this](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data> data)
        {
            if (ec1 || ec2)
            {
                BOOST_LOG_TRIVIAL(error) << " decode error: ec1:" << ec1.message() << " ec2" << csi::kafka::to_string(ec2) << std::endl;
                return;
            }
            for (std::vector<std::shared_ptr<csi::kafka::basic_message>>::const_iterator i = data->messages.begin(); i != data->messages.end(); ++i)
            {
                boost::uuids::uuid uuid;

                try
                {
                    std::string key((const char*) &(*i)->key[0], (*i)->key.size());
                    uuid = boost::uuids::string_generator()(key);
                    std::shared_ptr<std::string> handle = std::make_shared<std::string>((const char*) &(*i)->value[0], (*i)->value.size());
                    BOOST_LOG_TRIVIAL(info) << "offset: " << (*i)->offset << ", loaded: " << to_string(uuid) << " -> " << *handle;
                    _store.put(uuid, handle);
                }
                catch (std::exception& e)
                {
                    BOOST_LOG_TRIVIAL(warning) << "decode exception: " << e.what();
                    return;
                }
            }
        });
    }

    /// Handle a request and produce a reply.
    void handle_post_request(std::shared_ptr<csi::http::connection> context)
    {
        if (context->request().method() == csi::http::POST)
        {
            BOOST_LOG_TRIVIAL(debug) << to_string(context->request().content()) << std::endl;

            schema_registry_post_subjects_request_t request;
            try
            {
                csi::avro_json_decode(context->request().content(), request);
            }
            catch (std::exception& e)
            {
                BOOST_LOG_TRIVIAL(warning) << "avro_json_decode exception: " << e.what();
                context->reply().create(csi::http::bad_request);
                return;
            }

            try
            {
                // build a valid schema from input...
                const avro::ValidSchema validSchema(avro::compileJsonSchemaFromString(request.schema));
                boost::uuids::uuid uuid = generate_hash(validSchema);
                std::shared_ptr<std::string> handle = std::make_shared<std::string>(normalize(validSchema));

                _store.put(uuid, handle);
                _producer.send_async(std::shared_ptr<csi::kafka::basic_message>(new csi::kafka::basic_message(to_string(uuid), *handle)));

                schema_registry_post_subjects_response_t response;
                response.uuid = to_string(uuid);
                csi::avro_json_encode(response, context->reply().content());
                context->reply().create(csi::http::ok);
                return;
            }
            catch (std::exception& e)
            {
                BOOST_LOG_TRIVIAL(warning) << "compileJsonSchemaFromString exception: " << e.what();
                context->reply().create(csi::http::bad_request);
                return;
            }
        }
        else
        {
            context->reply().create(csi::http::bad_request);
        }
    }

    void handle_get_request(const std::string& id, std::shared_ptr<csi::http::connection> context)
    {
        if (context->request().method() == csi::http::GET)
        {
            boost::uuids::uuid uuid;
            try
            {
                uuid = boost::uuids::string_generator()(id);
            }
            catch (std::exception& e)
            {
                BOOST_LOG_TRIVIAL(warning) << "parse uuid exception: " << e.what();
                context->reply().create(csi::http::bad_request);
                return;
            }

            std::shared_ptr<std::string> handle = _store.get(uuid);
            if (!handle)
            {
                context->reply().create(csi::http::not_found);
                return;
            }

            schema_registry_get_subjects_response_t response;
            response.schema = *handle;
            csi::avro_json_encode(response, context->reply().content());
            context->reply().create(csi::http::ok);
        }
        else
        {
            context->reply().create(csi::http::bad_request);
        }
    }

    csi::kafka::highlevel_consumer                      _consumer;
    csi::kafka::highlevel_producer                      _producer;
    csi::kafka::table<boost::uuids::uuid, std::string>  _store;
};

int main(int argc, char** argv)
{
#ifdef WIN32
    std::string my_address = "127.0.0.1";
#else
    std::string my_address = "0.0.0.0";
#endif
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
    int http_port = 8081;
    std::vector<csi::kafka::broker_address> brokers;

    if (argc > 1)
        my_address = argv[1];

    size_t last_colon = my_address.find_last_of(':');
    if (last_colon != std::string::npos)
        http_port = atoi(my_address.substr(last_colon + 1).c_str());

    my_address = my_address.substr(0, last_colon);


    int32_t kafka_port = (argc > 3) ? atoi(argv[3]) : 9092;

    if (argc > 2)
    {
        brokers.push_back(csi::kafka::broker_address(argv[2], kafka_port));
    }
    else
    {
        brokers.push_back(csi::kafka::broker_address("192.168.0.102", kafka_port)); // my home cluster...
        brokers.push_back(csi::kafka::broker_address("127.0.0.1", kafka_port));
    }

    boost::asio::io_service ios;
    try
    {
        std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(ios));
        boost::thread bt(boost::bind(&boost::asio::io_service::run, &ios));

        schema_registry             registry(ios, "_uuid_schema");
        registry.connect(brokers);  
        csi::http::http_server      s1(ios, my_address, http_port);

        s1.add_handler("/subjects", [&registry](const std::vector<std::string>&, std::shared_ptr<csi::http::connection> c)
        {
            registry.handle_post_request(c);
        });

        s1.add_handler("/subjects/+", [&registry](const std::vector<std::string>& s, std::shared_ptr<csi::http::connection> c)
        {
            registry.handle_get_request(s[2], c);
        });


        while (true)
        {
            boost::this_thread::sleep(boost::posix_time::seconds(1000));
        }
    }
    catch (std::exception& e)
    {
        BOOST_LOG_TRIVIAL(error) << "exception: " << e.what() << " : exiting";
    }
    ios.stop();
    return 0;
}


