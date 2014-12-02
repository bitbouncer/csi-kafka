#pragma once

#include <memory>
#include <csi_kafka/kafka.h>

namespace csi
{
    namespace kafka
    {
        std::shared_ptr<produce_response>           parse_produce_response(const char* buffer, size_t len);
        std::shared_ptr<fetch_response>             parse_fetch_response(const char* buffer, size_t len);
        std::shared_ptr<offset_response>            parse_offset_response(const char* buffer, size_t len);
        std::shared_ptr<metadata_response>          parse_metadata_response(const char* buffer, size_t len);
        std::shared_ptr<offset_commit_response>     parse_offset_commit_response(const char* buffer, size_t len);
        std::shared_ptr<offset_fetch_response>      parse_offset_fetch_response(const char* buffer, size_t len);
        std::shared_ptr<consumer_metadata_response> parse_consumer_metadata_response(const char* buffer, size_t len);
    }
}