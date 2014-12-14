#include <algorithm>
#include <iostream>
#include <boost/crc.hpp>
#include "high_level_producer.h"

namespace csi
{
    namespace kafka
    {
        highlevel_producer::highlevel_producer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic) :
            _ios(io_service),
            _meta_client(io_service, query),
            _topic_name(topic)
        {
        }

        void highlevel_producer::close()
        {
            _meta_client.close(); 

            for (std::map<int, async_producer*>::iterator i = _producers.begin(); i != _producers.end(); ++i)
            {
                int broker_id = i->first;
                int partition = i->second->partition();
                i->second->close();
            }
        }

        boost::system::error_code highlevel_producer::connect()
        {
            boost::system::error_code ec = _meta_client.connect();

            if (ec)
                return ec;

            _metadata = _meta_client.get_metadata({ _topic_name }, 0);

            if (_metadata)
            {
                std::cerr << "metatdata for topic " << _topic_name << " failed" << std::endl;
                return  make_error_code(boost::system::errc::no_message);
            }

            for (std::vector<csi::kafka::broker_data>::const_iterator i = _metadata->brokers.begin(); i != _metadata->brokers.end(); ++i)
            {
                _brokers[i->node_id] = *i;
            };

/*
for (std::vector<csi::kafka::broker_data>::const_iterator i = _metadata->brokers.begin(); i != _metadata->brokers.end(); ++i)
            {
                boost::asio::ip::tcp::resolver::query query(i->host_name, std::to_string(i->port));
                _producers.insert(std::make_pair(i->node_id, new producer(_ios, query, _topic_name)));
                std::cerr << "broker #" << i->node_id << i->host_name << ":" << i->port << std::endl;
            };
*/


            for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = _metadata->topics.begin(); i != _metadata->topics.end(); ++i)
            {
                assert(i->topic_name == _topic_name);
                for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                {
                    std::map<int, broker_data>::const_iterator item = _brokers.find(j->leader);
                    if (item != _brokers.end())
                    {
                        _partitions[j->partition_id] = *j;

                        boost::asio::ip::tcp::resolver::query query(item->second.host_name, std::to_string(item->second.port));
                        _producers.insert(std::make_pair(j->partition_id, new async_producer(_ios, query, _topic_name, j->partition_id)));
                        std::cerr << "partition " << i->topic_name << ":" << j->partition_id << " -> " << j->leader << std::endl;
                    }
                    else
                    {
                        std::cerr << "leader not found  " << i->topic_name << ":" << j->partition_id << " -> " << j->leader << std::endl;
                    }
                };
            };

            for (std::map<int, async_producer*>::iterator i = _producers.begin(); i != _producers.end(); ++i)
            {
                int partition = i->first;
                int leader = _partitions[partition].leader;
                //int partition = i->second->partition();
                std::string broker_uri = _brokers[leader].host_name + ":" + std::to_string(_brokers[leader].port);
                
                std::cerr << "connecting to broker #" << leader << " (" << broker_uri << ") partition " << partition << std::endl;
                i->second->connect_async([leader, partition, broker_uri](const boost::system::error_code& ec1)
                {
                    if (ec1)
                    {
                        std::cerr << "can't connect to broker #" << leader << " (" << broker_uri << ") partition " << partition << " ec:" << ec1 << std::endl;
                    }
                    else
                        std::cerr << "connected to broker #" << leader << " (" << broker_uri << ") partition " << partition << std::endl;
                });
            }
            return ec;
        }

        void highlevel_producer::refresh_metadata_async()
        {
            _meta_client.get_metadata_async({ _topic_name }, 0, [this](rpc_result<metadata_response> result)
            {
                _metadata = result;
            });
        }

        void highlevel_producer::enqueue(std::shared_ptr<basic_message> message)
        {
            // calc a hash to get partition
            //for now use crc32 hardcoded 
            
            uint32_t hash = 0;
            if (!message->key.is_null())
            {
                boost::crc_32_type result;
                uint32_t keysize = (uint32_t) message->key.size();
                result.process_bytes(&message->key[0], message->key.size());
                hash = result.checksum();
            }
            else
            {
                std::cerr << " no key -> enque in parition 0 FIXME" << std::endl;
            }

            uint32_t partition = hash % _partitions.size();
            _producers[partition]->enqueue(message);
            std::cerr << "encqueue -> " << partition << " items:" << _producers[partition]->items_in_queue() << ", buffer:" << _producers[partition]->bytes_in_queue()/1024 << " KB " <<  std::endl;
        }
    };
};
