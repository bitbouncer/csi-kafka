#pragma once

#include <csi_kafka/kafka.h>

namespace csi
{
    namespace kafka
    {
        size_t encode_produce_request(const std::string& topic, int partition_id, int required_acks, int timeout, const std::vector<basic_message>& v, int32_t correlation_id, char* buffer, size_t capacity);
        size_t encode_metadata_request(const std::vector<std::string>& topics, int32_t correlation_id, char* buffer, size_t capacity);
        size_t encode_simple_fetch_request(const std::string& topic, int32_t partition_id, int64_t fetch_offset, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id, char* buffer, size_t capacity);
        size_t encode_multi_fetch_request(const std::string& topic, const std::vector<partition_cursor>& cursors, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id, char* buffer, size_t capacity);
        size_t encode_simple_offset_request(const std::string& topic, int32_t partition_id, int64_t time, int32_t max_number_of_offsets, int32_t correlation_id, char* buffer, size_t capacity);
        size_t encode_consumer_metadata_request(const std::string& consumer_group, int32_t correlation_id, char* buffer, size_t capacity);
        size_t encode_simple_offset_commit_request(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int64_t offset, int64_t timestamp, const std::string& metadata, int32_t correlation_id, char* buffer, size_t capacity);
        size_t encode_simple_offset_fetch_request(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int32_t correlation_id, char* buffer, size_t capacity);

        size_t encode_offset_fetch_all_request(const std::string& consumer_group, int32_t correlation_id, char* buffer, size_t capacity);
    }
}