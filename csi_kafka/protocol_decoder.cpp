#include <boost/asio.hpp>
#include <boost/endian/arithmetic.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/crc.hpp>

//#include <iostream>

#include "protocol_decoder.h"

namespace csi
{
    namespace kafka
    {
        namespace internal
        {
            inline void decode_i08(std::istream& src, uint8_t* dst)  { src.read((char*)dst, 1); }
            inline void decode_i16(std::istream& src, int16_t& dst) { u_short data; src.read((char*)&data, 2); dst = (int16_t)ntohs(data); }
            inline void decode_i32(std::istream& src, int32_t& dst) { u_long data;  src.read((char*)&data, 4); dst = (int32_t)ntohl(data); }
            inline void decode_i64(std::istream& src, int64_t& dst) { boost::endian::big_int64_t data; src.read((char*)&data, 8);  dst = data; }

            // all arrays & strings can be NULL also which is different from empty...
            inline void decode_string(std::istream& src, std::string& dst)
            {
                int16_t len;
                decode_i16(src, len);
                if (len < 0)
                    len = 0;
                if (len == 0)
                {
                    dst = "";
                    return;
                }
                dst.reserve(len);
                std::istream_iterator<char> isi(src);
                std::copy_n(isi, len, std::insert_iterator<std::string>(dst, dst.begin()));
            }

            inline void decode_arr(std::istream& src, std::vector<uint8_t>& dst)
            {
                int32_t len;
                decode_i32(src, len);

                if (len < 0)
                    len = 0;

                dst.reserve(len);

                uint8_t x;
                for (int i = 0; i != len; ++i)
                {
                    decode_i08(src, &x);
                    dst.push_back(x);
                }
            }

            inline void decode_arr(std::istream& src, csi::kafka::basic_message::payload_type& value)
            {
                int32_t len;
                decode_i32(src, len);

                if (len < 0)
                {
                    value.set_null(true);
                    return;
                }

                value.set_null(false);
                value.reserve(len);

                uint8_t x;
                for (int i = 0; i != len; ++i)
                {
                    decode_i08(src, &x);
                    value.push_back(x);
                }
            }

            inline void decode_arr(std::istream& src, std::vector<int32_t>& dst)
            {
                int32_t len;
                decode_i32(src, len);

                if (len < 0)
                    len = 0;

                dst.reserve(len);

                int32_t x;
                for (int i = 0; i != len; ++i)
                {
                    decode_i32(src, x);
                    dst.push_back(x);
                }
            }

            inline void decode_arr(std::istream& src, std::vector<int64_t>& dst)
            {
                int32_t len;
                decode_i32(src, len);

                if (len < 0)
                    len = 0;

                dst.reserve(len);

                int64_t x;
                for (int i = 0; i != len; ++i)
                {
                    decode_i64(src, x);
                    dst.push_back(x);
                }
            }
        };

        //ProduceResponse = >[TopicName[Partition ErrorCode Offset]]
        //TopicName = > string
        //Partition = > int32
        //ErrorCode = > int16
        //Offset = > int64
        rpc_result<produce_response> parse_produce_response(const char* buffer, size_t len)
        {
            rpc_result<produce_response> response(new produce_response());
            boost::iostreams::stream<boost::iostreams::array_source> str(buffer, len);

            if (len == 0)
            {
                response.ec.ec1 = make_error_code(boost::system::errc::bad_message);
                return response;
            }

            internal::decode_i32(str, response->correlation_id);
            int32_t nr_of_topics;
            internal::decode_i32(str, nr_of_topics);
            response->topics.reserve(nr_of_topics);
            for (int i = 0; i != nr_of_topics; ++i)
            {
                produce_response::topic_data item;
                internal::decode_string(str, item.topic_name);

                int32_t items;
                internal::decode_i32(str, items);
                item.partitions.reserve(items);
                for (int j = 0; j != items; ++j)
                {
                    produce_response::topic_data::partition_data pr_item;
                    internal::decode_i32(str, pr_item.partition_id);
                    internal::decode_i16(str, pr_item._error_code);
                    internal::decode_i64(str, pr_item.offset); // should we read this field if error_code != 0 ???? TBD KOLLA
                    item.partitions.push_back(pr_item);
                }
                response->topics.push_back(item);
            }
            return response;
        }

        static void parse_message_set(const char* buffer, size_t len, std::vector<std::shared_ptr<basic_message>>& v)
        {
            boost::iostreams::stream<boost::iostreams::array_source> str(buffer, len);

            while (!str.eof())
            {
                std::shared_ptr<basic_message> item = std::make_shared<basic_message>();
                internal::decode_i64(str, item->offset);

                int32_t message_size;
                internal::decode_i32(str, message_size);

                // stream good & do we have enough bytes in stream to decode this?
                if (!str.good() || ((len - str.tellg()) < message_size))
                    break;

                // verify crc
                {
                    uint32_t expected_crc;
                    internal::decode_i32(str, *(int32_t*)&expected_crc);

                    std::streampos cursor = str.tellg();
                    boost::crc_32_type result;
                    result.process_bytes(buffer + cursor, message_size - 4);
                    uint32_t actual_crc = result.checksum();
                    if (expected_crc != actual_crc)
                        break;
                }

                int8_t magic_byte = 0;
                int8_t attributes = 0;
                str.read((char*)&magic_byte, 1);
                str.read((char*)&attributes, 1);

                internal::decode_arr(str, item->key);
                internal::decode_arr(str, item->value);

                //uncompressed? then this is an actual value
                if (attributes == 0)
                {
                    v.push_back(item);
                }
                else
                {
                    // this is compressed - uncompress and parse this again as a message set...
                    assert(false);
                }
            }
        }

        //FetchResponse = >[TopicName[Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
        //TopicName = > string
        //Partition = > int32
        //ErrorCode = > int16
        //HighwaterMarkOffset = > int64
        //MessageSetSize = > int32
        rpc_result<fetch_response> parse_fetch_response(const char* buffer, size_t len)
        {
            boost::iostreams::stream<boost::iostreams::array_source> str(buffer, len);

            rpc_result<fetch_response> response(new fetch_response());

            internal::decode_i32(str, response->correlation_id);

            int16_t resulting_error_code = 0;
            int32_t nr_of_topics;
            internal::decode_i32(str, nr_of_topics);
            response->topics.reserve(nr_of_topics);

            for (int i = 0; i != nr_of_topics; ++i)
            {
                fetch_response::topic_data topic_item;
                internal::decode_string(str, topic_item.topic_name);

                int32_t nr_of_partitions;
                internal::decode_i32(str, nr_of_partitions);
                topic_item.partitions.reserve(nr_of_partitions);
                for (int j = 0; j != nr_of_partitions; ++j)
                {
                    std::shared_ptr<fetch_response::topic_data::partition_data> partition_data = std::make_shared<fetch_response::topic_data::partition_data>();
                    internal::decode_i32(str, partition_data->partition_id);
                    internal::decode_i16(str, partition_data->error_code);
                    if (partition_data->error_code)
                        resulting_error_code = partition_data->error_code;
                    internal::decode_i64(str, partition_data->highwater_mark_offset);  // ska vi läsa detta om error code är !=0 TBD
                    int32_t  message_set_size = 0;
                    internal::decode_i32(str, message_set_size); // ska vi läsa detta om error code är !=0 TBD

                    //it's possible that we have a partial message - dont trust message_set_size
                    size_t bytes_to_parse = std::min<size_t>(message_set_size, len - str.tellg());
                    if (!str.good())
                        break;
                    parse_message_set(buffer + str.tellg(), bytes_to_parse, partition_data->messages);
                    str.ignore(bytes_to_parse); // we must skip this block
                    topic_item.partitions.push_back(partition_data);
                }
                response->topics.push_back(topic_item);

                // here stream might be broken
                if (!str.good())
                    break;
            }
            response.ec.ec2 = (csi::kafka::error_codes) resulting_error_code;
            return response;
        }

        //OffsetResponse = >[TopicName[PartitionOffsets]]
        //PartitionOffsets = > Partition ErrorCode[Offset]
        //Partition = > int32
        //ErrorCode = > int16
        //Offset = > int64
        rpc_result<offset_response> parse_offset_response(const char* buffer, size_t len)
        {
            boost::iostreams::stream<boost::iostreams::array_source> str(buffer, len);
            rpc_result<offset_response> response(new offset_response());
            internal::decode_i32(str, response->correlation_id);

            int32_t nr_of_topics;
            internal::decode_i32(str, nr_of_topics);
            response->topics.reserve(nr_of_topics);
            for (int i = 0; i != nr_of_topics; ++i)
            {
                offset_response::topic_data item;
                internal::decode_string(str, item.topic_name);

                int32_t nr_of_partitions;
                internal::decode_i32(str, nr_of_partitions);
                item.partitions.reserve(nr_of_partitions);
                for (int j = 0; j != nr_of_partitions; ++j)
                {
                    offset_response::topic_data::partition_data pd;
                    internal::decode_i32(str, pd.partition_id);
                    internal::decode_i16(str, pd.error_code);
                    internal::decode_arr(str, pd.offsets);
                    item.partitions.push_back(pd);
                }
                response->topics.push_back(item);
            }
            return response;
        }

        //MetadataResponse = >[Broker][TopicMetadata]
        //Broker = > NodeId Host Port
        //NodeId = > int32
        //Host = > string
        //Port = > int32
        //TopicMetadata = > TopicErrorCode TopicName[PartitionMetadata]
        //TopicErrorCode = > int16
        //PartitionMetadata = > PartitionErrorCode PartitionId Leader Replicas Isr
        //PartitionErrorCode = > int16
        //PartitionId = > int32
        //Leader = > int32
        //Replicas = >[int32]
        //Isr = >[int32]
        rpc_result<metadata_response> parse_metadata_response(const char* buffer, size_t len)
        {
            boost::iostreams::stream<boost::iostreams::array_source> str(buffer, len);
            rpc_result<metadata_response> response(new metadata_response());
            internal::decode_i32(str, response->correlation_id);

            int32_t nr_of_brokers;
            internal::decode_i32(str, nr_of_brokers);
            response->brokers.reserve(nr_of_brokers);
            for (int i = 0; i != nr_of_brokers; ++i)
            {
                broker_data item;
                internal::decode_i32(str, item.node_id);
                internal::decode_string(str, item.host_name);
                internal::decode_i32(str, item.port);
                response->brokers.push_back(item);
            }


            int32_t nr_of_topics;
            internal::decode_i32(str, nr_of_topics);
            response->topics.reserve(nr_of_topics);
            for (int i = 0; i != nr_of_topics; ++i)
            {
                metadata_response::topic_data item;
                internal::decode_i16(str, item.error_code);
                internal::decode_string(str, item.topic_name);

                int32_t nr_of_partitions;
                internal::decode_i32(str, nr_of_partitions);
                item.partitions.reserve(nr_of_partitions);

                for (int j = 0; j != nr_of_partitions; ++j)
                {
                    metadata_response::topic_data::partition_data pd;
                    internal::decode_i16(str, pd.error_code);
                    internal::decode_i32(str, pd.partition_id);
                    internal::decode_i32(str, pd.leader);
                    internal::decode_arr(str, pd.replicas);
                    internal::decode_arr(str, pd.isr);
                    item.partitions.push_back(pd);
                }
                response->topics.push_back(item);
            }
            return response;
        }

        //OffsetCommitResponse = >[TopicName[Partition ErrorCode]]]
        //TopicName = > string
        //Partition = > int32
        //ErrorCode = > int16
        rpc_result<offset_commit_response> parse_offset_commit_response(const char* buffer, size_t len)
        {
            boost::iostreams::stream<boost::iostreams::array_source> str(buffer, len);
            rpc_result<offset_commit_response> response(new offset_commit_response());
            internal::decode_i32(str, response->correlation_id);

            int32_t nr_of_topics;
            internal::decode_i32(str, nr_of_topics);
            response->topics.reserve(nr_of_topics);
            for (int i = 0; i != nr_of_topics; ++i)
            {
                offset_commit_response::topic_data item;
                internal::decode_string(str, item.topic_name);

                int32_t nr_of_partitions;
                internal::decode_i32(str, nr_of_partitions);
                item.partitions.reserve(nr_of_partitions);
                for (int j = 0; j != nr_of_partitions; ++j)
                {
                    offset_commit_response::topic_data::partition_data pd;
                    internal::decode_i32(str, pd.partition_id);
                    internal::decode_i16(str, pd.error_code);
                    item.partitions.push_back(pd);
                }
                response->topics.push_back(item);
            }
            return response;
        }

        //OffsetFetchResponse = >[TopicName[Partition Offset Metadata ErrorCode]]
        //TopicName = > string
        //Partition = > int32
        //Offset = > int64
        //Metadata = > string
        //ErrorCode = > int16
        rpc_result<offset_fetch_response> parse_offset_fetch_response(const char* buffer, size_t len)
        {
            boost::iostreams::stream<boost::iostreams::array_source> str(buffer, len);
            rpc_result<offset_fetch_response> response(new offset_fetch_response());
            internal::decode_i32(str, response->correlation_id);

            int32_t nr_of_topics;
            internal::decode_i32(str, nr_of_topics);
            response->topics.reserve(nr_of_topics);
            for (int i = 0; i != nr_of_topics; ++i)
            {
                offset_fetch_response::topic_data item;
                internal::decode_string(str, item.topic_name);

                int32_t nr_of_partitions;
                internal::decode_i32(str, nr_of_partitions);
                item.partitions.reserve(nr_of_partitions);
                for (int j = 0; j != nr_of_partitions; ++j)
                {
                    offset_fetch_response::topic_data::partition_data pd;
                    internal::decode_i32(str, pd.partition_id);
                    internal::decode_i64(str, pd.offset);
                    internal::decode_string(str, pd.metadata);
                    internal::decode_i16(str, pd.error_code);
                    item.partitions.push_back(pd);
                }
                response->topics.push_back(item);
            }
            return response;
        }

        // bad spec!!
        //ClusterMetadataResponse = > ErrorCode | CoordinatorId CoordinatorHost CoordinatorPort |
        //ErrorCode = > int16
        //CoordinatorId = > int32
        //CoordinatorHost = > string
        //CoordinatorPort = > int32
        rpc_result<cluster_metadata_response> parse_cluster_metadata_response(const char* buffer, size_t len)
        {
            boost::iostreams::stream<boost::iostreams::array_source> str(buffer, len);
            rpc_result<cluster_metadata_response> response(new cluster_metadata_response());
            internal::decode_i32(str, response->correlation_id);
            internal::decode_i16(str, response->error_code);

            if (response->error_code == 0)
            {
                internal::decode_i32(str, response->coordinator_id);

                int32_t nr_of_brokers;
                internal::decode_i32(str, nr_of_brokers);
                response->brokers.reserve(nr_of_brokers);
                for (int i = 0; i != nr_of_brokers; ++i)
                {
                    broker_data item;
                    internal::decode_i32(str, item.node_id);
                    internal::decode_string(str, item.host_name);
                    internal::decode_i32(str, item.port);
                    response->brokers.push_back(item);
                }
            }
            return response;
        }
    }
}