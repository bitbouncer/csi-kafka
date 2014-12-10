#include "consumer.h"
#include <boost/thread.hpp>
#include <boost/bind.hpp>

namespace csi
{
    namespace kafka
    {
        lowlevel_consumer::lowlevel_consumer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic) :
            _ios(io_service),
            _client(io_service, query),
            _topic_name(topic)
        {
        }

        void lowlevel_consumer::connect_async(connect_callback cb)
        {
            _client.connect_async(cb);
        }

        boost::system::error_code lowlevel_consumer::connect()
        {
            return _client.connect();
        }

        void lowlevel_consumer::set_offset_async(int32_t partition, int64_t start_time, set_offset_callback cb)
        {
            // cant be done twice
            for (std::vector<partition_cursor>::const_iterator i = _cursors.begin(); i != _cursors.end(); ++i)
            {
                assert(i->_partition_id != partition);
            }

            _client.perform_async(csi::kafka::create_simple_offset_request(_topic_name, partition, start_time, 10, 0), [this, partition, cb](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
            {
                if (ec)
                    cb(ec, csi::kafka::NoError);

                auto response = csi::kafka::parse_offset_response(handle);
                for (std::vector<csi::kafka::offset_response::topic_data>::const_iterator i = response->topics.begin(); i != response->topics.end(); ++i)
                {
                    // this should always be true.
                    assert(i->topic_name == _topic_name);
                    if (i->topic_name == _topic_name)
                    {
                        assert(i->partitions.size() == 1);
                        for (std::vector<csi::kafka::offset_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                        {
                            assert(j->partition_id == partition);
                            if (j->partition_id == partition)
                            {
                                if (j->offsets.size())
                                {
                                    // must lock if multithreaded
                                    _cursors.emplace_back(partition, j->offsets[0]);
                                }
                                cb(ec, (csi::kafka::error_codes) j->error_code);
                                return;
                            }
                        }
                    }
                }
                cb(ec, csi::kafka::error_codes::Unknown); // this should never happen
            });
        }

        std::pair<boost::system::error_code, csi::kafka::error_codes> lowlevel_consumer::set_offset(int32_t partition, int64_t start_time)
        {
            std::promise<std::pair<boost::system::error_code, csi::kafka::error_codes>> p;
            std::future<std::pair<boost::system::error_code, csi::kafka::error_codes>>  f = p.get_future();
            set_offset_async(partition, start_time, [&p](const boost::system::error_code& ec1, csi::kafka::error_codes ec2)
            {
                p.set_value(std::make_pair(ec1, ec2));
            });
            f.wait();
            return f.get();
        }

        void lowlevel_consumer::stream_async(datastream_callback cb)
        {
            _client.perform_async(csi::kafka::create_multi_fetch_request(_topic_name, _cursors, 100, 10, 0), [this, cb](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
            {
                if (ec)
                {
                    csi::kafka::fetch_response::topic_data::partition_data dummy;
                    cb(ec, csi::kafka::NoError, dummy);
                    return;
                }

                auto response = csi::kafka::parse_fetch_response(handle);
                for (std::vector<csi::kafka::fetch_response::topic_data>::const_iterator i = response->topics.begin(); i != response->topics.end(); ++i)
                {
                    // this should always be true.
                    if (i->topic_name == _topic_name)
                    {
                        for (std::vector<csi::kafka::fetch_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                        {
                            for (std::vector<partition_cursor>::iterator k = _cursors.begin(); k != _cursors.end(); ++k)
                            {
                                if (j->partition_id == k->_partition_id)  // a partition that have been closed will not exist here so it will not be added again in the next read loop 
                                {
                                    if (j->messages.size())
                                        k->_next_offset = j->messages[j->messages.size() - 1].offset + 1;
                                    cb(ec, ((csi::kafka::error_codes) j->error_code), *j); // possibly partition & ack j->messages[j->messages.size() - 1].offset here or send it to application
                                }
                            }
                        }
                    }
                }
                stream_async(cb);
            });
        }
    
        void lowlevel_consumer::get_metadata_async(get_metadata_callback cb)
        {
            _client.perform_async(csi::kafka::create_metadata_request({ _topic_name }, 0), [cb](const boost::system::error_code& ec, csi::kafka::basic_call_context::handle handle)
            {
                if (ec)
                {
                    std::cerr << "fetch_metadata_async failed: ec:" << ec;
                    cb(ec, csi::kafka::NoError, NULL);
                    return;
                }

                 auto response = csi::kafka::parse_metadata_response(handle);
                 cb(ec, csi::kafka::NoError, response);
            });
        }
    } // kafka
}; // csi