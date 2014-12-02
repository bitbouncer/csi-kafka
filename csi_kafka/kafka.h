/*
*    the documentation
*    https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
*
*/

#pragma once

#include <stdint.h>
#include <vector>
#include <string>

namespace csi
{
    namespace kafka
    {
        enum { ApiVersion = 0 };

        enum { latest_offsets = -1, earliest_available_offset = -2 };

        enum error_codes
        {
            NoError = 0,                    // No error--it worked!
            Unknown = -1,                   // An unexpected server error
            OffsetOutOfRange = 1,           // The requested offset is outside the range of offsets maintained by the server for the given topic / partition.
            InvalidMessage = 2,             // This indicates that a message contents does not match its CRC
            UnknownTopicOrPartition = 3,    // This request is for a topic or partition that does not exist on this broker.
            InvalidMessageSize = 4,         // The message has a negative size
            LeaderNotAvailable = 5,         // This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
            NotLeaderForPartition = 6,      // This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition.It indicates that the clients metadata is out of date.
            RequestTimedOut = 7,            // This error is thrown if the request exceeds the user-specified time limit in the request.
            BrokerNotAvailable = 8,         // This is not a client facing error and is used only internally by intra-cluster broker communication.
            Unused = 9,                     // Unused
            MessageSizeTooLarge = 10,       // The server has a configurable maximum message size to avoid unbounded memory allocation.This error is thrown if the client attempt to produce a message larger than this maximum.
            StaleControllerEpochCode = 11,  // Internal error code for broker-to-broker communication.
            OffsetMetadataTooLargeCode = 12 // If you specify a string larger than configured maximum for offset metadata
        };

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
                inline void set_string(const char* ch)                    { set((const uint8_t*) ch, strlen(ch)); }
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
            int64_t                 offset;
            payload_type            key;
            payload_type            value;

            /*
            basic_message() : offset(0), _key_is_null(false), _value_is_null(false) {}
            basic_message(const std::string& akey, const std::string& aval) : offset(0), _key_is_null(false), _value_is_null(false), _key(akey.begin(), akey.end()), _value(aval.begin(), aval.end()) {}

            inline bool is_key_null() const { return _key_is_null; }
            inline bool is_value_null() const { return _value_is_null; }
            inline const std::vector<uint8_t>& key() const { return _key; }
            inline const std::vector<uint8_t>& value() const { return _value; }

            void set_key(const uint8_t* buf, size_t len)   { _key.reserve(len); for (size_t i = 0; i != len; ++i) _key.push_back(buf[i]); _key_is_null = false; }
            void set_value(const uint8_t* buf, size_t len) { _value.reserve(len); for (size_t i = 0; i != len; ++i) _value.push_back(buf[i]); _value_is_null = false; }

            int64_t                 offset;
            bool                    _key_is_null;
            bool                    _value_is_null;
            std::vector<uint8_t>    _key;
            std::vector<uint8_t>    _value;
            */
        };

        //ProduceResponse = >[TopicName[Partition ErrorCode Offset]]
        struct produce_response
        {
            produce_response() : correlation_id(-1) {}
            struct topic_data
            {
                struct partition_data
                {
                    partition_data() : partition_id(-1), error_code(-1), offset(-1) {}
                    int32_t partition_id;
                    int16_t error_code;
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
    };
};