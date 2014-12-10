#include <algorithm>
#include <iostream>
#include "high_level_consumer.h"

namespace csi
{
    namespace kafka
    {
        highlevel_consumer::highlevel_consumer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic) :
            _ios(io_service),
            _meta_client(io_service, query),
            _topic_name(topic)
        {
        }

        boost::system::error_code highlevel_consumer::connect()
        {
            boost::system::error_code ec = _meta_client.connect();

            if (ec)
                return ec;

            _metadata = _meta_client.get_metadata({ _topic_name }, 0);

            if (!_metadata)
                return  make_error_code(boost::system::errc::no_message);

            for (std::vector<csi::kafka::broker_data>::const_iterator i = _metadata->brokers.begin(); i != _metadata->brokers.end(); ++i)
            {
                boost::asio::ip::tcp::resolver::query query(i->host_name, std::to_string(i->port));
                _consumers.insert(std::make_pair(i->node_id, new lowlevel_consumer(_ios, query, _topic_name)));
                std::cerr << "broker #" << i->node_id << i->host_name << ":" << i->port << std::endl;
            };

            for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = _metadata->topics.begin(); i != _metadata->topics.end(); ++i)
            {
                assert(i->topic_name == _topic_name);
                for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                {
                    std::cerr << "partition " << i->topic_name << ":" << j->partition_id << " -> " << j->leader << std::endl;
                };
            };

            for (std::map<int, lowlevel_consumer*>::iterator i = _consumers.begin(); i != _consumers.end(); ++i)
            {
                int broker_id = i->first;
                i->second->connect_async([broker_id](const boost::system::error_code& ec1)
                {
                    if (ec1)
                        std::cerr << "can't connect to broker #" << broker_id << " ec:" << ec1 << std::endl;
                    else
                        std::cerr << "connected to broker #" << broker_id << std::endl;
                });
            }
            return ec;
        }

        void highlevel_consumer::refresh_metadata_async()
        {
            _meta_client.get_metadata_async({ _topic_name }, 0, [this](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, std::shared_ptr<metadata_response> p)
            {
                _metadata = p;
            });
        }



    };
};
