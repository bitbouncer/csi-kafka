#include "async_metadata_client.h"
#include <iostream>

namespace csi
{
    namespace kafka
    {
        async_metadata_client::async_metadata_client(boost::asio::io_service& io_service) :
            _ios(io_service),
            _metadata_timer(io_service),
            _connect_retry_timer(io_service),
            _metadata_timeout(boost::posix_time::milliseconds(10000)),
            _current_retry_timeout(boost::posix_time::milliseconds(1000)),
            _max_retry_timeout(boost::posix_time::milliseconds(60000)),
            _client(io_service)
        {
        }

        async_metadata_client::~async_metadata_client()
        {
            _metadata_timer.cancel();
            _connect_retry_timer.cancel();
            _client.close();
        }

        void async_metadata_client::close()
        {
            _client.close();
        }

        bool async_metadata_client::is_connected() const
        {
            return _client.is_connected();
        }

        void async_metadata_client::connect_async(const std::vector<broker_address>& brokers, connect_callback cb)
        {
            _known_brokers = brokers;
            _connect_cb = cb;
            _next_broker = _known_brokers.begin();
            _connect_async_next();
        }

        boost::system::error_code async_metadata_client::connect(const std::vector<broker_address>& brokers)
        {
            std::promise<boost::system::error_code> p;
            std::future<boost::system::error_code>  f = p.get_future();
            connect_async(brokers, [&p](const boost::system::error_code& error)
            {
                p.set_value(error);
            });
            f.wait();
            return f.get();
        }

        void async_metadata_client::_connect_async_next()
        {
            std::cerr << "_connect_async_next " << _next_broker->host_name << ":" << _next_broker->port << std::endl;
            boost::asio::ip::tcp::resolver::query query(_next_broker->host_name, std::to_string(_next_broker->port));
            _next_broker++;

            if (_next_broker == _known_brokers.end())
                _next_broker = _known_brokers.begin();

            _client.connect_async(query, 1000, [this](const boost::system::error_code& ec)
            {
                if (ec)
                {
                    _connect_retry_timer.expires_from_now(_current_retry_timeout);
                    _connect_retry_timer.async_wait(boost::bind(&async_metadata_client::handle_connect_retry_timer, this, boost::asio::placeholders::error));
                }
                else
                {
                    _client.get_metadata_async({ "dummy_topic" }, 99, boost::bind(&async_metadata_client::handle_get_metadata, this, _1));
                }
            });
        }


        void async_metadata_client::handle_connect_retry_timer(const boost::system::error_code& ec)
        {
            if (!ec)
            {
                std::cerr << "handle_connect_retry_timer" << std::endl;
                _current_retry_timeout + boost::posix_time::seconds(1);
                if (_current_retry_timeout > _max_retry_timeout)
                    _current_retry_timeout = _max_retry_timeout;
                _connect_async_next();
            }
        }

        void async_metadata_client::handle_get_metadata_timer(const boost::system::error_code& ec)
        {
            if (!ec)
            {
                //std::cerr << "handle_get_metadata_timer" << std::endl;
                _client.get_metadata_async({ "dummy_topic" }, 99, boost::bind(&async_metadata_client::handle_get_metadata, this, _1));
            }
        }

        void async_metadata_client::handle_get_metadata(rpc_result<metadata_response> response)
        {
            //std::cerr << "handle_get_metadata" << std::endl;
            if (response.ec)
            {
                _client.close();
                _connect_retry_timer.expires_from_now(_current_retry_timeout);
                _connect_retry_timer.async_wait(boost::bind(&async_metadata_client::handle_connect_retry_timer, this, boost::asio::placeholders::error));
            }
            else
            {
                csi::kafka::spinlock::scoped_lock xxx(_spinlock);
                _metadata = response;

                for (std::vector<csi::kafka::broker_data>::const_iterator i = _metadata->brokers.begin(); i != _metadata->brokers.end(); ++i)
                {
                    _broker2brokers[i->node_id] = *i;
                };

                // any changes to broker list?
                bool changed = false;

                changed = _metadata->brokers.size() != _known_brokers.size();
                
                if (!changed)
                {
                    // see if we find all existing brokers in new list
                    for (std::vector<csi::kafka::broker_data>::const_iterator i = _metadata->brokers.begin(); i != _metadata->brokers.end(); ++i)
                    {
                        bool found = false;
                        for (std::vector<broker_address>::const_iterator j = _known_brokers.begin(); j != _known_brokers.end(); ++j)
                        {
                            if (i->host_name == j->host_name && i->port == j->port)
                            {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            changed = true;
                            break;
                        }
                    }
                }

                if (changed)
                {
                    std::cerr << "known brokers changed { ";
                    _known_brokers.clear();
                    for (std::vector<csi::kafka::broker_data>::const_iterator i = _metadata->brokers.begin(); i != _metadata->brokers.end(); ++i)
                    {
                        std::cerr << i->host_name << ":" << i->port << ", ";
                        _known_brokers.push_back(broker_address(i->host_name, i->port));
                    }
                    std::cerr << " } " << std::endl;
                    _next_broker = _known_brokers.begin();
                }

                if (_connect_cb)
                {
                    _connect_cb(response.ec.ec1);
                    _connect_cb = NULL;
                }

                _metadata_timer.expires_from_now(_metadata_timeout);
                _metadata_timer.async_wait(boost::bind(&async_metadata_client::handle_get_metadata_timer, this, boost::asio::placeholders::error));
            }
        }

        void async_metadata_client::get_metadata_async(const std::vector<std::string>& topics, int32_t correlation_id, get_metadata_callback cb)
        {
            _client.get_metadata_async(topics, correlation_id, cb);
        }

        rpc_result<metadata_response> async_metadata_client::get_metadata(const std::vector<std::string>& topics, int32_t correlation_id)
        {
            return _client.get_metadata(topics, correlation_id);
            /*
            if (_client.is_connected())
                return _client.get_metadata(topics, correlation_id);
            else
                return  rpc_result<metadata_response>(make_error_code(boost::system::errc::not_connected));
            */
        }
        };

};