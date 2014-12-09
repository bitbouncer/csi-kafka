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

            //we could loop over all hosta and create a connection that handles severla partitions. 
            //however our low_level_client handler currentlyu only one partition per client.

            if (!_metadata)
                return  make_error_code(boost::system::errc::no_message);

            for (std::vector<csi::kafka::broker_data>::const_iterator i = _metadata->brokers.begin(); i != _metadata->brokers.end(); ++i)
            {
                std::cerr << "broker #" << i->node_id << i->host_name << ":" << i->port << std::endl;
            };

            for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = _metadata->topics.begin(); i!=_metadata->topics.end(); ++i)
            {
                assert(i->topic_name == _topic_name);
                for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j!=i->partitions.end(); ++j)
                {
                    std::cerr << "partition " << i->topic_name << ":" << j->partition_id << " -> " << j->leader << std::endl;



                };
            };

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
