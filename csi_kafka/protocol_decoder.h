#pragma once

#include <memory>
#include <csi_kafka/kafka.h>

namespace csi
{
    namespace kafka
    {
        rpc_result<produce_response>           parse_produce_response(const char* buffer, size_t len);
        rpc_result<fetch_response>             parse_fetch_response(const char* buffer, size_t len);
        rpc_result<offset_response>            parse_offset_response(const char* buffer, size_t len);
        rpc_result<metadata_response>          parse_metadata_response(const char* buffer, size_t len);
        rpc_result<offset_commit_response>     parse_offset_commit_response(const char* buffer, size_t len);
        rpc_result<offset_fetch_response>      parse_offset_fetch_response(const char* buffer, size_t len);
        rpc_result<cluster_metadata_response>  parse_cluster_metadata_response(const char* buffer, size_t len);
    }
}