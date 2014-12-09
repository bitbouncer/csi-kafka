#include "consumer.h"
#include <boost/thread.hpp>
#include <boost/bind.hpp>

namespace csi
{
    namespace kafka
    {
        lowlevel_consumer::lowlevel_consumer(boost::asio::io_service& io_service, const boost::asio::ip::tcp::resolver::query& query, const std::string& topic, int32_t partition) :
            _ios(io_service),
            _client(io_service, query),
            _topic_name(topic),
            _partition_id(partition),
            _next_offset(csi::kafka::earliest_available_offset)
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

        void lowlevel_consumer::set_offset_async(int64_t start_time, set_offset_callback cb)
        {
            _client.perform_async(csi::kafka::create_simple_offset_request(_topic_name, _partition_id, start_time, 10, 0), [this, cb](csi::kafka::error_codes ec, csi::kafka::basic_call_context::handle handle)
            {
                _next_offset = csi::kafka::earliest_available_offset;

                if (ec)
                    cb(ec);

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
                            assert(j->partition_id == _partition_id);
                            if (j->partition_id == _partition_id)
                            {
                                if (j->offsets.size())   // vad betyder det att få flera offsets här???
                                    _next_offset = j->offsets[0];
                                cb((csi::kafka::error_codes) j->error_code);
                                return;
                            }
                        }
                    }
                }
                cb(csi::kafka::error_codes::Unknown); // this should never happen
            });
        }

        csi::kafka::error_codes lowlevel_consumer::set_offset(int64_t start_time)
        {
            std::promise<csi::kafka::error_codes> p;
            std::future<csi::kafka::error_codes>  f = p.get_future();
            set_offset_async(start_time, [&p](csi::kafka::error_codes error)
            {
                p.set_value(error);
            });
            f.wait();
            return f.get();
        }

        void lowlevel_consumer::get_next_data_async(datastream_callback cb)
        {
            _client.perform_async(csi::kafka::create_simple_fetch_request(_topic_name, _partition_id, 100, 10, _next_offset, 0), [this, cb](csi::kafka::error_codes ec, csi::kafka::basic_call_context::handle handle)
            {
                if (ec)
                {
                    csi::kafka::fetch_response::topic_data::partition_data dummy;
                    cb(ec, dummy);
                    return;
                }

                auto response = csi::kafka::parse_fetch_response(handle);
                for (std::vector<csi::kafka::fetch_response::topic_data>::const_iterator i = response->topics.begin(); i != response->topics.end(); ++i)
                {
                    // this should always be true.
                    if (i->topic_name == _topic_name)
                    {
                        assert(i->partitions.size() == 1); // for now
                        for (std::vector<csi::kafka::fetch_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
                        {
                            assert(j->partition_id == _partition_id);
                            // should always be true
                            if (j->partition_id == _partition_id)
                            {
                                if (j->messages.size())
                                    _next_offset = j->messages[j->messages.size() - 1].offset + 1;
                                cb(((csi::kafka::error_codes) j->error_code), *j);
                                return; // what if there is more then we would need a new callback but that breaks the async contract - if we should support this the we need a stream contract
                            }
                        }
                    }
                }

                csi::kafka::fetch_response::topic_data::partition_data dummy;
                cb(csi::kafka::error_codes::Unknown, dummy); // should never happen
            });
        }

        void lowlevel_consumer::open_stream(datastream_callback cb)
        {
            get_next_data_async([this, cb](csi::kafka::error_codes error, const csi::kafka::fetch_response::topic_data::partition_data& pd)
            {
                cb(error, pd);
                if (!error) // we could check close_stream here and break the loop
                    open_stream(cb);
            });
        }

        void lowlevel_consumer::get_metadata_async(get_metadata_callback cb)
        {
            _client.perform_async(csi::kafka::create_metadata_request({ _topic_name }, 0), [cb](csi::kafka::error_codes error, csi::kafka::basic_call_context::handle handle)
            {
                if (!error)
                {
                    auto response = csi::kafka::parse_metadata_response(handle);
                    cb(error, response);
                }
                else
                {
                    std::cerr << "fetch_metadata_async failed: " << to_string(error);
                    cb(error, NULL);
                }
            });
        }

        ///*
        //void lowlevel_consumer::_on_retry_timer(const boost::system::error_code& ec)
        //{
        //    if (!ec)
        //    {
        //        if (_state == CONNETING_TO_CLUSTER)
        //        {
        //        }
        //        /*
        //        else if (_state == CONNECTING_TO_PARTION_LEADER)
        //        {

        //        }
        //        */
        //    }
        //}
        //*/

        /*
        void lowlevel_consumer::_on_cluster_connect(const boost::system::error_code& ec)
        {
        if (ec)
        {
        // retry in one second...
        // set timer
        }

        if (!ec)
        {
        _state = GETTING_METADATA;
        _client.perform_async(csi::kafka::create_metadata_request({ "test" }, 0), boost::bind(&lowlevel_consumer::_on_metadata_request, this, _1, _2));
        }

        }
        */

        /*
        void lowlevel_consumer::_on_metadata_request(csi::kafka::error_codes ec, csi::kafka::basic_call_context::handle handle)
        {
        auto response = csi::kafka::parse_metadata_response(handle);
        _brokers = response->brokers;

        // add the brokers to the known host in the cluster TODO

        _partition_leader = -1;
        for (std::vector<csi::kafka::metadata_response::topic_data>::const_iterator i = response->topics.begin(); i != response->topics.end(); ++i)
        {
        if (i->topic_name == _topic_name)
        {
        if (i->error_code)
        {
        std::cerr << "topic error from metadata request " << i->error_code << std::endl; // should we retry here??
        }
        else
        {
        for (std::vector<csi::kafka::metadata_response::topic_data::partition_data>::const_iterator j = (*i).partitions.begin(); j != (*i).partitions.end(); ++j)
        {
        if (j->partition_id == _partition_id)
        {
        if (j->error_code != 0)
        {
        std::cerr << "partition error from metadata request " << j->error_code << std::endl; // should we retry here??
        }
        else
        {
        _partition_leader = j->leader;
        }
        }
        }
        }
        }
        }

        // not in use right now since we only have one server - difficult to test and reslies on stuff in client that we have not yet written...

        // we found our partition leader - let's connect to that node instead.
        //if (_partition_leader >= 0)
        //{
        //    for (std::vector<csi::kafka::broker_data>::const_iterator i = response->brokers.begin(); i != response->brokers.end(); ++i)
        //    {
        //        if (i->node_id == _partition_leader)
        //        {
        //            _client.connect();
        // CONNECTING_TO_PARTION_LEADER
        //        }
        //    }
        //}
        _state = CONNECTING_TO_PARTION_LEADER;
        boost::system::error_code ec2;
        _ios.post(boost::bind(&lowlevel_consumer::_on_leader_connect, this, ec2));
        }
        */

        //void lowlevel_consumer::_on_leader_connect(const boost::system::error_code& ec)
        //{
        //    if (ec)
        //    {
        //        // retry in one second...
        //        // set timer
        //    }

        //    if (!ec)
        //    {
        //        _state = READY;
        //        _client.perform_async(csi::kafka::create_simple_offset_request(_topic_name, _partition_id, _start_point_in_time, 10, 0), boost::bind(&lowlevel_consumer::_on_offset_request, this, _1, _2));
        //        //_client.perform_async(csi::kafka::create_simple_offset_request(_topic_name, _partition_id, csi::kafka::earliest_available_offset, 10, 0), boost::bind(&consumer::_on_offset_request, this, _1, _2));
        //    }
        //}


        //void lowlevel_consumer::_on_fetch_data(csi::kafka::error_codes ec, csi::kafka::basic_call_context::handle handle)
        //{
        //    if (ec)
        //    {
        //        return;
        //    }

        //    auto response = csi::kafka::parse_fetch_response(handle);
        //    if (_datastream_callback)
        //    {
        //        for (std::vector<csi::kafka::fetch_response::topic_data>::const_iterator i = response->topics.begin(); i != response->topics.end(); ++i)
        //        {
        //            // this should always be true.
        //            if (i->topic_name == _topic_name)
        //            {
        //                for (std::vector<csi::kafka::fetch_response::topic_data::partition_data>::const_iterator j = i->partitions.begin(); j != i->partitions.end(); ++j)
        //                {
        //                    _datastream_callback(*j);
        //                    if (j->messages.size())
        //                        _next_offset = j->messages[j->messages.size() - 1].offset + 1;
        //                }
        //            }
        //        }
        //    }
        //    _client.perform_async(csi::kafka::create_simple_fetch_request(_topic_name, _partition_id, 100, 10, _next_offset, 0), boost::bind(&lowlevel_consumer::_on_fetch_data, this, _1, _2));
        //}
    } // kafka
}; // csi
