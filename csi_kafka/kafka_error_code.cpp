#include "kafka_error_code.h"

namespace csi
{
    namespace kafka
    {
        std::string to_string(error_codes error)
        {
            switch (error)
            {
            case Unknown: return "Unknown (An unexpected server error)"; // -1
            case NoError: return "NoError (OK)";                         // 0
            case OffsetOutOfRange: return "OffsetOutOfRange";
            case InvalidMessage: return "InvalidMessage (Bad CRC)";
            case UnknownTopicOrPartition: return "UnknownTopicOrPartition";
            case InvalidFetchSizeCode: return "InvalidFetchSizeCode";
            case LeaderNotAvailable: return "LeaderNotAvailable";
            case NotLeaderForPartition: return "NotLeaderForPartition";
            case RequestTimedOut: return "RequestTimedOut";
            case BrokerNotAvailable: return "BrokerNotAvailable";
            case ReplicaNotAvailable: return "ReplicaNotAvailable";
            case MessageSizeTooLarge: return "MessageSizeTooLarge";
            case StaleControllerEpochCode: return "StaleControllerEpochCode";
            case OffsetMetadataTooLargeCode: return "OffsetMetadataTooLargeCode";
            case NetworkException: return "The server disconnected before a response was received.";
            case OffsetsLoadInProgressCode: return "OffsetsLoadInProgressCode";
            case ConsumerCoordinatorNotAvailableCode: return "ConsumerCoordinatorNotAvailableCode";
            case NotCoordinatorForConsumerCode: return "NotCoordinatorForConsumerCode";
            case InvalidTopicCode: return "The request attempted to perform an operation on an invalid topic.";
            case MessageSetSizeTooLargeCode: return "The request included message batch larger than the configured segment size on the server";
            case NotEnoughReplicasCode: return "Messages are rejected since there are fewer in-sync replicas than required.";
            case NotEnoughReplicasAfterAppendCode: return "Messages are written to the log, but to fewer in-sync replicas than required.";
            case InvalidRequiredAcks: return "Produce request specified an invalid value for required acks";
            case IllegalConsumerGeneration: return "Specified consumer generation id is not valid.";
            case NoOffsetsCommittedCode: return "No offsets have been committed so far";
            default:
                return "Undefined error #" + std::to_string((int)error);
            }
        }

        std::string to_string(const rpc_error_code& e)
        {
            return std::string("system:") + e.ec1.message() + ", kafka:" + to_string(e.ec2);
        }

    };
};