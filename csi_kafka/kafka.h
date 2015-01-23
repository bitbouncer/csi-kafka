/*
*    the documentation
*    https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
*
*/

#pragma once

#include <stdint.h>
#include <vector>
#include <cstring>
#include <string>
#include <memory>
#include "kafka_error_code.h"

namespace csi
{
    namespace kafka
    {
        struct broker_address
        {
            broker_address() : port(-1) {}
            broker_address(const std::string& h, int32_t p) : host_name(h), port(p) {}
            std::string host_name;
            int32_t     port;
        };

        std::string to_string(const broker_address&);

        enum { ApiVersion = 0 };

        enum { latest_offsets = -1, earliest_available_offset = -2 };



        enum api_keys
        {
            ProduceRequest = 0,
            FetchRequest = 1,
            OffsetRequest = 2,
            MetadataRequest = 3,
            Internal4 = 4,
            Internal5 = 5,
            Internal6 = 6,
            Internal7 = 7,
            OffsetCommitRequest = 8,
            OffsetFetchRequest = 9,
            ConsumerMetadataRequest = 10
        };

        //top level types used by api
        struct broker_data
        {
            broker_data() : node_id(-1), port(-1) {}
            int32_t     node_id;
            std::string host_name;
            int32_t     port;
        };



        //MessageSet = >[Offset MessageSize Message]
        //    Offset = > int64
        //    MessageSize = > int32

        //    Message format
        //    Message = > Crc MagicByte Attributes Key Value
        //    Crc = > int32
        //    MagicByte = > int8
        //    Attributes = > int8
        //    Key = > bytes
        //    Value = > bytes


        struct basic_message
        {
            class payload_type
            {
            public:
                payload_type() : _is_null(true) {}
                payload_type(const uint8_t* begin, const uint8_t* end) : _value(begin, end), _is_null(false) {}
                payload_type(const char* buf, size_t len) : _value(buf, buf + len), _is_null(false) {}
                inline void set(const uint8_t* begin, const uint8_t* end) { _value.reserve(end - begin); _value.assign(begin, end); _is_null = false; }
                inline void set(const uint8_t* begin, size_t len)         { _value.reserve(len); _value.assign(begin, begin + len); _is_null = false; }
                inline void set_string(const char* ch)                    { set((const uint8_t*)ch, strlen(ch)); }
                inline void reserve(size_t len)                           { _value.reserve(len); }
                inline void push_back(uint8_t ch)                         { _value.push_back(ch); _is_null = false; }
                inline void set_null(bool val)                            { _is_null = val; }
                inline bool is_null() const                               { return _is_null; }
                inline const std::vector<uint8_t>& value() const          { return _value; }
                inline size_t size() const                                { return _value.size(); }

                inline uint8_t& operator[] (const size_t index)           { return _value[index]; }
                inline const uint8_t& operator[] (const size_t index) const { return _value[index]; }

            private:
                std::vector<uint8_t> _value;
                bool                 _is_null;
            };

            basic_message() : offset(0) {}
            basic_message(const std::string& akey, const std::string& aval) : offset(0), key(akey.data(), akey.size()), value(aval.data(), aval.size()) {}
            size_t size() const { return key.size() + value.size() + 26; } // estimated size for streaming TODO check if this is correct

            int64_t                 offset;
            payload_type            key;
            payload_type            value;
        };

        struct partition_cursor
        {
            inline partition_cursor(int32_t partition, int64_t offset) : _partition_id(partition), _next_offset(offset) {}
            int32_t _partition_id;
            int64_t _next_offset;
        };

        //ProduceResponse = >[TopicName[Partition ErrorCode Offset]]
        struct produce_response
        {
            produce_response() : correlation_id(-1) {}
            struct topic_data
            {
                struct partition_data
                {
                    partition_data() : partition_id(-1), _error_code(0), offset(-1) {}
                    inline error_codes error_code() const { return (error_codes)_error_code; }
                    int32_t partition_id;
                    int16_t _error_code;
                    int64_t offset;
                };

                std::string                 topic_name;
                std::vector<partition_data> partitions;
            };
            int32_t                 correlation_id;
            std::vector<topic_data> topics;
        };

        //FetchResponse = >[TopicName[Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
        struct fetch_response
        {
            fetch_response() : correlation_id(-1) {}
            struct topic_data
            {
                struct partition_data
                {
                    partition_data() : partition_id(-1), error_code(-1), highwater_mark_offset(-1) {}
                    int32_t                    partition_id;
                    int16_t                    error_code;
                    int64_t                    highwater_mark_offset;
                    std::vector<basic_message> messages;
                };
                std::string                 topic_name;
                std::vector<partition_data> partitions;
            };
            int32_t                 correlation_id;
            std::vector<topic_data> topics;
        };

        //OffsetResponse = >[TopicName[PartitionOffsets]]
        //PartitionOffsets = > Partition ErrorCode[Offset]
        //Partition = > int32
        //ErrorCode = > int16
        //Offset = > int64
        struct offset_response
        {
            offset_response() : correlation_id(-1) {}
            struct topic_data
            {
                struct partition_data
                {
                    partition_data() : partition_id(-1), error_code(-1) {}
                    int32_t                 partition_id;
                    int16_t                 error_code;
                    std::vector<int64_t>    offsets;
                };
                std::string                 topic_name;
                std::vector<partition_data> partitions;
            };
            int32_t                 correlation_id;
            std::vector<topic_data> topics;
        };

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
        struct metadata_response
        {
            metadata_response() : correlation_id(-1) {}

            struct topic_data
            {
                topic_data() : error_code(-1) {}
                struct partition_data
                {
                    partition_data() : error_code(-1), partition_id(-1), leader(-1) {}
                    int16_t              error_code;
                    int32_t              partition_id;
                    int32_t              leader;
                    std::vector<int32_t> replicas;
                    std::vector<int32_t> isr;
                };

                int16_t                     error_code;
                std::string                 topic_name;
                std::vector<partition_data> partitions;
            };
            int32_t                     correlation_id;
            std::vector<broker_data>    brokers;
            std::vector<topic_data>     topics;
        };

        //OffsetCommitResponse = >[TopicName[Partition ErrorCode]]]
        struct offset_commit_response
        {
            offset_commit_response() : correlation_id(-1) {}
            struct topic_data
            {
                struct partition_data
                {
                    partition_data() : partition_id(-1), error_code(-1) {}
                    int32_t partition_id;
                    int16_t error_code;
                };
                std::string                 topic_name;
                std::vector<partition_data> partitions;
            };
            int32_t                 correlation_id;
            std::vector<topic_data> topics;
        };

        //OffsetFetchResponse = >[TopicName[Partition Offset Metadata ErrorCode]]
        struct offset_fetch_response
        {
            offset_fetch_response() : correlation_id(-1) {}
            struct topic_data
            {
                struct partition_data
                {
                    partition_data() : partition_id(-1), offset(-1), error_code(-1) {}
                    int32_t     partition_id;
                    int64_t     offset;
                    std::string metadata;
                    int16_t     error_code;
                };

                std::string                 topic_name;
                std::vector<partition_data> partitions;
            };
            int32_t                     correlation_id;
            std::vector<topic_data>     topics;
        };

        //ConsumerMetadataResponse = > ErrorCode | CoordinatorId CoordinatorHost CoordinatorPort |
        struct consumer_metadata_response
        {
            consumer_metadata_response() : correlation_id(-1), error_code(-1), coordinator_id(-1), port(-1) {}
            int32_t     correlation_id;
            int16_t     error_code;
            int32_t     coordinator_id;
            std::string host_name;
            int32_t     port;
        };


        // More message types below
        //https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design

        /*
        JoinGroupRequest
        {
        GroupId = > String
        SessionTimeout = > int32
        Topics = >[String]
        ConsumerId = > String
        PartitionAssignmentStrategy = > String
        }

        JoinGroupResponse
        {
        ErrorCode = > int16
        GroupGenerationId = > int32
        ConsumerId = > String
        PartitionsToOwn = >[TopicName[Partition]]
        }
        TopicName = > String
        Partition = > int32

        HeartbeatRequest
        {
        GroupId = > String
        GroupGenerationId = > int32
        ConsumerId = > String
        }
        HeartbeatResponse
        {
        ErrorCode = > int16
        }

        OffsetCommitRequest(v1)

        OffsetCommitRequest = > ConsumerGroup GroupGenerationId ConsumerId[TopicName[Partition Offset TimeStamp Metadata]]
        ConsumerGroup = > string
        GroupGenerationId = > int32
        ConsumerId = > String
        TopicName = > string
        Partition = > int32
        Offset = > int64
        TimeStamp = > int64
        Metadata = > string
        */

        template <class T>
        struct rpc_result
        {
            inline rpc_result() {}
            inline rpc_result(const rpc_error_code& e) : ec(e) {}
            inline rpc_result(T* d) : data(d) {}
            inline rpc_result(const rpc_error_code& e, std::shared_ptr<T> d) : ec(e), data(d) {}


            inline operator bool() const  BOOST_SYSTEM_NOEXCEPT // true if error
            {
                return ec || (data.get() == NULL);
            }

            inline bool operator!() const  BOOST_SYSTEM_NOEXCEPT // true if no error
            {
                return !ec && (data.get() != NULL);
            }

            inline T* operator->() const { return data.get(); }

            rpc_error_code     ec;
            std::shared_ptr<T> data;
        };
    };
};
