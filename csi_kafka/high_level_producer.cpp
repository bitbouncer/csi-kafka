#include <algorithm>
#include <boost/crc.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <csi_kafka/internal/async.h>
#include "high_level_producer.h"

namespace csi
{
    namespace kafka
    {
        highlevel_producer::highlevel_producer(boost::asio::io_service& io_service, const std::string& topic, int32_t required_acks, int32_t tx_timeout, int32_t max_packet_size) :
            _ios(io_service),
            _timer(io_service),
            _timeout(boost::posix_time::milliseconds(5000)),
            _meta_client(io_service),
            _topic(topic),
            _required_acks(required_acks),
            _tx_timeout(tx_timeout),
            _max_packet_size(max_packet_size)
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

        void highlevel_producer::connect_async(const std::vector<broker_address>& brokers)
        {
            _meta_client.connect_async(brokers, [this](const boost::system::error_code& ec)
            {
                _ios.post([this]{ _try_connect_brokers(); });
            });
        }

        void highlevel_producer::_try_connect_brokers()
        {
            // the number of partitions is constant but the serving hosts might differ
            _meta_client.get_metadata_async({ _topic }, 0, [this](rpc_result<metadata_response> result)
            {
                if (!result)
                {
                    {
                        csi::kafka::spinlock::scoped_lock xxx(_spinlock);
                        //_metadata = result;

                        //changes?
                        //TODO we need to create new producers if we have more partitions
                        //TODO we cant shrink cluster at the moment...
                        if (_partition2producers.size() == 0)
                        {
                            for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = result->topics.begin(); i != result->topics.end(); ++i)
                            {
                                assert(i->topic_name == _topic);
                                if (i->error_code)
                                {
                                    BOOST_LOG_TRIVIAL(warning) << "metadata for topic " << _topic << " failed: " << to_string((error_codes)i->error_code);
                                }
                                for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                                    _partition2producers.insert(std::make_pair(j->partition_id, new async_producer(_ios, _topic, j->partition_id, _required_acks, _tx_timeout, _max_packet_size)));
                            }

                            //lets send all things that were collected before connecting
                            std::deque<tx_item> ::reverse_iterator cursor = _tx_queue.rbegin();
                           
                            while (_tx_queue.size())
                            {
                                tx_item& item = *(_tx_queue.end() - 1);
                                uint32_t partition = item.hash % _partition2producers.size();
                                _partition2producers[partition]->send_async(item.msg, item.cb);
                                _tx_queue.pop_back();
                            }
                        }


                        for (std::vector<csi::kafka::broker_data>::const_iterator i = result->brokers.begin(); i != result->brokers.end(); ++i)
                        {
                            _broker2brokers[i->node_id] = *i;
                        };

                        for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = result->topics.begin(); i != result->topics.end(); ++i)
                        {
                            assert(i->topic_name == _topic);
                            for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                            {
                                _partition2partitions[j->partition_id] = *j;
                            };
                        };
                    }

                    for (std::map<int, async_producer*>::iterator i = _partition2producers.begin(); i != _partition2producers.end(); ++i)
                    {
                        if (!i->second->is_connected() && !i->second->is_connection_in_progress())
                        {

                            int partition = i->first;
                            int leader = _partition2partitions[partition].leader;
                            auto bd = _broker2brokers[leader];
                            broker_address addr(bd.host_name, bd.port);
                            i->second->close();
                            BOOST_LOG_TRIVIAL(info) << "connecting to broker node_id:" << leader << " (" << to_string(addr) << ") partition:" << partition;
                            i->second->connect_async(addr, 1000, [leader, partition, addr](const boost::system::error_code& ec1)
                            {
                                if (ec1)
                                {
                                    BOOST_LOG_TRIVIAL(warning) << "can't connect to broker #" << leader << " (" << to_string(addr) << ") partition " << partition << " ec:" << ec1;
                                }
                                else
                                {
                                    BOOST_LOG_TRIVIAL(info) << "connected to broker #" << leader << " (" << to_string(addr) << ") partition " << partition;
                                }
                            });
                        }
                    }
                }

                _timer.expires_from_now(_timeout);
                _timer.async_wait(boost::bind(&highlevel_producer::handle_timer, this, boost::asio::placeholders::error));
            });
        }

        void highlevel_producer::send_async(std::shared_ptr<basic_message> message, tx_ack_callback cb)
        {
            // calc a hash to get partition
            //for now use crc32 hardcoded 

            uint32_t hash = 0;
            if (!message->key.is_null())
            {
                boost::crc_32_type result;
                uint32_t keysize = (uint32_t)message->key.size();
                result.process_bytes(&message->key[0], message->key.size());
                hash = result.checksum();
            }
            else
            {
                BOOST_LOG_TRIVIAL(error) << " no key -> enque in parition 0 FIXME";
            }

            // enqueu in partition queue or store if we don't have a connection the cluster.
            csi::kafka::spinlock::scoped_lock xxx(_spinlock);
            
            // TBD change below when we can accept repartitioning - for now it's okay to store initial data and send it when we find the cluster.
            //if (_meta_client.is_connected() && _partition2producers.size())
            if (_partition2producers.size())
            {
                uint32_t partition = hash % _partition2producers.size();
                _partition2producers[partition]->send_async(message, cb);
            }
            else
            {
                _tx_queue.push_front(tx_item(hash, message, cb));
            }
        }

        void highlevel_producer::send_async(std::vector<std::shared_ptr<basic_message>>& messages, tx_ack_callback cb)
        {
            for (std::vector<std::shared_ptr<basic_message>>::const_iterator i = messages.begin(); i != messages.end(); ++i)
                send_async(*i);
            size_t partitions = _partition2producers.size();
            std::shared_ptr<csi::async::destructor_callback> final_cb(new csi::async::destructor_callback(cb));
            for (int i = 0; i != partitions; ++i)
            {
                _partition2producers[i]->send_async(NULL, [i, final_cb]()
                {
                });
            }
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
                item.tx_roundtrip = (*i).second->metrics_tx_roundtrip();
                metrics.push_back(item);
            }
            return metrics;
        }
    };
};
