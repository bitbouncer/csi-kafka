#include <algorithm>
#include <iostream>
#include <boost/crc.hpp>
#include "high_level_producer.h"

namespace csi
{
    namespace kafka
    {
        highlevel_producer::highlevel_producer(boost::asio::io_service& io_service, const std::string& topic) :
            _ios(io_service),
            _timer(io_service),
            _timeout(boost::posix_time::milliseconds(5000)),
            _meta_client(io_service),
            _topic_name(topic)
        {
        }

        highlevel_producer::~highlevel_producer()
        {
            _timer.cancel();
        }

        void highlevel_producer::handle_timer(const boost::system::error_code& ec)
        {
            if (!ec)
                _try_connect_brokers();
        }

        void highlevel_producer::close()
        {
            _timer.cancel();

            _meta_client.close(); 

            for (std::map<int, async_producer*>::iterator i = _partition2producers.begin(); i != _partition2producers.end(); ++i)
            {
                i->second->close();
            }
        }

        boost::system::error_code highlevel_producer::connect(const boost::asio::ip::tcp::resolver::query& query)
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

            for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = _metadata->topics.begin(); i != _metadata->topics.end(); ++i)
            {
                assert(i->topic_name == _topic_name);
                for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                    _partition2producers.insert(std::make_pair(j->partition_id, new async_producer(_ios, _topic_name, j->partition_id)));
            };

            _ios.post([this]{ _try_connect_brokers(); });
        
            return ec;
        }

        void highlevel_producer::_try_connect_brokers()
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

                for (std::map<int, async_producer*>::iterator i = _partition2producers.begin(); i != _partition2producers.end(); ++i)
                {
                    if (!i->second->is_connected() && !i->second->is_connection_in_progress())
                    {
                        
                        int partition = i->first;
                        int leader = _partition2partitions[partition].leader;
                        auto bd = _broker2brokers[leader];
                        boost::asio::ip::tcp::resolver::query query(bd.host_name, std::to_string(bd.port));
                        std::string broker_uri = bd.host_name + ":" + std::to_string(bd.port);
                        i->second->close();
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

                _timer.expires_from_now(_timeout);
                _timer.async_wait(boost::bind(&highlevel_producer::handle_timer, this, boost::asio::placeholders::error));
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

            uint32_t partition = hash % _partition2producers.size();
            //uint32_t partition = 1;
            _partition2producers[partition]->enqueue(message);
            //std::cerr << "encqueue -> " << partition << " items:" << _partition2producers[partition]->items_in_queue() << ", buffer:" << _partition2producers[partition]->bytes_in_queue() / 1024 << " KB " << std::endl;
        }


        std::vector<highlevel_producer::metrics>  highlevel_producer::get_metrics() const
        {
            std::vector<metrics> metrics;
            for (std::map<int, async_producer*>::const_iterator i = _partition2producers.begin(); i != _partition2producers.end(); ++i)
            {
                highlevel_producer::metrics item;
                item.partition = (*i).second->partition();
                item.bytes_in_queue = (*i).second->bytes_in_queue();
                item.msg_in_queue = (*i).second->items_in_queue();
                item.tx_kb_sec = (*i).second->metrics_kb_sec();
                item.tx_msg_sec = (*i).second->metrics_msg_sec();
                metrics.push_back(item);
            }
            return metrics;
        }
    };
};
