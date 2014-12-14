#include <algorithm>
#include <iostream>
#include "high_level_consumer.h"

namespace csi
{
    namespace kafka
    {
        highlevel_consumer::highlevel_consumer(boost::asio::io_service& io_service, const std::string& topic) :
            _ios(io_service),
            _meta_client(io_service),
            _topic_name(topic)
        {
        }

        /*
        highlevel_consumer::~highlevel_consumer()
        {
            _timer.cancel();
        }
        
        void highlevel_consumer::handle_timer(const boost::system::error_code& ec)
        {
            if (!ec)
                _try_connect_brokers();
        }
        */

        boost::system::error_code highlevel_consumer::connect(const boost::asio::ip::tcp::resolver::query& query)
        {
            boost::system::error_code ec = _meta_client.connect(query);

            if (ec)
                return ec;

            _metadata = _meta_client.get_metadata({ _topic_name }, 0);

            if (_metadata)
            {
                std::cerr << "metatdata for topic " << _topic_name << " failed" << std::endl;
                return  make_error_code(boost::system::errc::no_message);
            }

            for (std::vector<csi::kafka::broker_data>::const_iterator i = _metadata->brokers.begin(); i != _metadata->brokers.end(); ++i)
                _consumers.insert(std::make_pair(i->node_id, new lowlevel_consumer(_ios, _topic_name)));

            _ios.post([this]{ _try_connect_brokers(); });

            return ec;
        }

        void highlevel_consumer::refresh_metadata_async()
        {
            _meta_client.get_metadata_async({ _topic_name }, 0, [this](rpc_result<metadata_response> result)
            {
                _metadata = result;
            });
        }


        void highlevel_consumer::_try_connect_brokers()
        {
            // the number of partitions is constand but the serving hosts might differ
            _meta_client.get_metadata_async({ _topic_name }, 0, [this](rpc_result<metadata_response> result)
            {
                if (!result)
                {
                    {
                        csi::kafka::spinlock::scoped_lock xxx(_spinlock);
                        _metadata = result;

                        for (std::vector<csi::kafka::broker_data>::const_iterator i = _metadata->brokers.begin(); i != _metadata->brokers.end(); ++i)
                        {
                            _broker2brokers[i->node_id] = *i;
                        };

                        for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = _metadata->topics.begin(); i != _metadata->topics.end(); ++i)
                        {
                            assert(i->topic_name == _topic_name);
                            for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                            {
                                _partition2partitions[j->partition_id] = *j;
                            };
                        };
                    }
                }

                for (std::map<int, lowlevel_consumer*>::iterator i = _consumers.begin(); i != _consumers.end(); ++i)
                {
                    if (!i->second->is_connected())
                    {
                        int partition = i->first;
                        int leader = _partition2partitions[partition].leader;
                        auto bd = _broker2brokers[leader];
                        boost::asio::ip::tcp::resolver::query query(bd.host_name, std::to_string(bd.port));
                        std::string broker_uri = bd.host_name + ":" + std::to_string(bd.port);
                        std::cerr << "connecting to broker node_id:" << leader << " (" << broker_uri << ") partition:" << partition << std::endl;
                        i->second->connect_async(query, [leader, partition, broker_uri](const boost::system::error_code& ec1)
                        {
                            if (ec1)
                            {
                                std::cerr << "can't connect to broker #" << leader << " (" << broker_uri << ") partition " << partition << " ec:" << ec1 << std::endl;
                            }
                            else
                                std::cerr << "connected to broker #" << leader << " (" << broker_uri << ") partition " << partition << std::endl;
                        });
                    }
                }
            });
        }
    };
};
