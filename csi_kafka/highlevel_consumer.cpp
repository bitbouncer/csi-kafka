#include <algorithm>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <csi_kafka/internal/async.h>
#include "highlevel_consumer.h"

namespace csi
{
    namespace kafka
    {
        highlevel_consumer::highlevel_consumer(boost::asio::io_service& io_service, const std::string& topic, int32_t rx_timeout, int32_t max_packet_size) :
            _ios(io_service),
            _timer(io_service),
            _timeout(boost::posix_time::milliseconds(5000)),
            _meta_client(io_service),
            _consumer_meta_client(io_service),
            _topic(topic),
            _rx_timeout(rx_timeout),
            _max_packet_size(max_packet_size)
        {
        }

        highlevel_consumer::~highlevel_consumer()
        {
            _timer.cancel();
        }

        void highlevel_consumer::handle_timer(const boost::system::error_code& ec)
        {
            if (!ec)
                _try_connect_brokers();
        }

        void highlevel_consumer::close()
        {
            _timer.cancel();
            _meta_client.close();
            _consumer_meta_client.close();
            for (std::map<int, lowlevel_consumer*>::iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i)
            {
                i->second->close();
            }
        }

        void highlevel_consumer::connect_forever(const std::vector<broker_address>& brokers)
        {
            _meta_client.connect_async(brokers, [this](const boost::system::error_code& ec)
            {
                _ios.post([this]{ _try_connect_brokers(); });
            });
        }

        void highlevel_consumer::connect_async(const std::vector<broker_address>& brokers, connect_callback cb)
        {

            BOOST_LOG_TRIVIAL(trace) << "HLC connect_async START";
            _meta_client.connect_async(brokers, [this, cb](const boost::system::error_code& ec)
            {
                BOOST_LOG_TRIVIAL(trace) << "HLC METADATA connect_async CB";
                if (!ec)
                {
                    BOOST_LOG_TRIVIAL(trace) << "HLC _connect_async STARTING";
                    _connect_async(cb);
                } // connect ok?
            }); //connect_async
        }

        void highlevel_consumer::_connect_async(connect_callback cb)
        {
            _meta_client.get_metadata_async({ _topic }, 0, [this, cb](rpc_result<metadata_response> result)
            {
                BOOST_LOG_TRIVIAL(trace) << "HLC _meta_client.get_metadata_async() CALLBACK ENTERED...";
                handle_response(result);
                if (!result)
                {
                    //std::vector < boost::function <void( const boost::system::error_code&)>> f;
                    //vector of async functions having callback on completion

                    std::shared_ptr<std::vector<csi::async::async_function>> work(new std::vector<csi::async::async_function>());

                    for (std::map<int, lowlevel_consumer*>::iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i)
                    {
                        if (!i->second->is_connected() && !i->second->is_connection_in_progress())
                        {
                            work->push_back([this, i](csi::async::async_callback cb)
                            {
                                int partition = i->first;
                                int leader = _partition2partitions[partition].leader;
                                auto bd = _broker2brokers[leader];
                                broker_address broker_addr(bd.host_name, bd.port);

                                i->second->connect_async(broker_addr, 1000, [this, leader, partition, broker_addr, cb](const boost::system::error_code& ec1)
                                {
                                    if (ec1)
                                    {
                                        BOOST_LOG_TRIVIAL(warning) << _topic << ":" << partition << ", HLC can't connect to broker #" << leader << " (" << to_string(broker_addr) << ") ec:" << ec1;
                                    }
                                    else
                                    {
                                        BOOST_LOG_TRIVIAL(info) << _topic << ":" << partition << ", HLC connected to broker #" << leader << " (" << to_string(broker_addr) << ")";
                                    }
                                    cb(ec1);
                                });
                            });
                        }
                    }

                    BOOST_LOG_TRIVIAL(trace) << "HLC connect_async / waterfall START";
                    csi::async::waterfall(*work, [work, cb](const boost::system::error_code& ec) // add iterator for last function
                    {
                        BOOST_LOG_TRIVIAL(trace) << "HLC connect_async / waterfall CB ec=" << ec;
                        if (ec)
                        {
                            BOOST_LOG_TRIVIAL(warning) << "HLC connect_async can't connect to broker ec:" << ec;
                        }
                        cb(ec);
                        BOOST_LOG_TRIVIAL(trace) << "HLC connect_async / waterfall EXIT";
                    }); //waterfall
                    BOOST_LOG_TRIVIAL(trace) << "HLC _meta_client.get_metadata_async() CB EXIT";
                } // get_metadata_async ok?
                else
                {
                    BOOST_LOG_TRIVIAL(trace) << "HLC _meta_client.get_metadata_async() error cb";
                    cb(result.ec.ec1);
                }
            });
        }

        boost::system::error_code highlevel_consumer::connect(const std::vector<broker_address>& brokers)
        {
            std::promise<boost::system::error_code> p;
            std::future<boost::system::error_code>  f = p.get_future();
            connect_async(brokers, [&p](const boost::system::error_code& error)
            {
                p.set_value(error);
            });
            f.wait();
            boost::system::error_code ec = f.get();
            if (ec)
            {
                BOOST_LOG_TRIVIAL(warning) << _topic << ", HLC can't connect to broker all brokers " << ec.message();
            }
            else
            {
                BOOST_LOG_TRIVIAL(info) << _topic << ", HLC connect to all brokers OK";
            }
            return ec;
        }

        void highlevel_consumer::set_offset(int64_t start_time)
        {
            // return value??? TBD what to do if this fails and if # partitions changes???
            for (std::map<int, lowlevel_consumer*>::iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i)
            {
                i->second->set_offset(start_time);
            }
        }

        void highlevel_consumer::_try_connect_brokers()
        {
            _connect_async([this](const boost::system::error_code& ec)
            {
                _timer.expires_from_now(_timeout);
                _timer.async_wait(boost::bind(&highlevel_consumer::handle_timer, this, boost::asio::placeholders::error));
            });
        }

        void highlevel_consumer::handle_response(rpc_result<metadata_response> result)
        {
            if (!result)
            {
                {
                    csi::kafka::spinlock::scoped_lock xxx(_spinlock);

                    if (_partition2consumers.size() == 0)
                    {
                        for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = result->topics.begin(); i != result->topics.end(); ++i)
                        {
                            assert(i->topic_name == _topic);
                            if (i->error_code)
                            {
                                BOOST_LOG_TRIVIAL(warning) << _topic << ", HLC get metadata failed: " << to_string((error_codes)i->error_code);
                            }
                            for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                            {
                                _partition2consumers.insert(std::make_pair(j->partition_id, new lowlevel_consumer(_ios, _topic, j->partition_id, _rx_timeout)));
                            }
                        };
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
            }
        }


        void  highlevel_consumer::stream_async(datastream_callback cb)
        {
            size_t partitions = _partition2consumers.size();
            for (int i = 0; i != partitions; ++i)
            {
                _partition2consumers[i]->stream_async(cb);
            }
        }

        void highlevel_consumer::fetch(fetch_callback cb)
        {
            auto final_cb = std::make_shared<csi::async::destructor_callback<std::vector<fetch_response>>>(cb);
            size_t partitions = _partition2consumers.size();
            for (int i = 0; i != partitions; ++i)
            {
                _partition2consumers[i]->fetch([final_cb](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data> data)
                {
                    fetch_response r;
                    r.ec1 = ec1;
                    r.ec2 = ec2;
                    r.data = data;
                    final_cb->value().push_back(r);
                });
            }
        }

        std::vector<highlevel_consumer::fetch_response> highlevel_consumer::fetch()
        {
            std::promise<std::vector<fetch_response>> p;
            std::future<std::vector<fetch_response>>  f = p.get_future();
            fetch([&p](std::vector<fetch_response>& v)
            {
                p.set_value(v);
            });
            f.wait();
            return f.get();
        }

        /*
        std::vector<int64_t> highlevel_consumer::get_offsets()
        {
        }
        */

        std::vector<highlevel_consumer::metrics>  highlevel_consumer::get_metrics() const
        {
            std::vector<metrics> metrics;
            for (std::map<int, lowlevel_consumer*>::const_iterator i = _partition2consumers.begin(); i != _partition2consumers.end(); ++i)
            {
                highlevel_consumer::metrics item;
                item.partition = (*i).second->partition();
                item.rx_kb_sec = (*i).second->metrics_kb_sec();
                item.rx_msg_sec = (*i).second->metrics_msg_sec();
                item.rx_roundtrip = (*i).second->metrics_rx_roundtrip();
                metrics.push_back(item);
            }
            return metrics;
        }
    };
};
