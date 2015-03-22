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
            case StaleLeaderEpochCode: return "StaleLeaderEpochCode";
            case OffsetsLoadInProgressCode: return "OffsetsLoadInProgressCode";
            case ConsumerCoordinatorNotAvailableCode: return "ConsumerCoordinatorNotAvailableCode";
            case NotCoordinatorForConsumerCode: return "NotCoordinatorForConsumerCode";
            case InvalidTopicCode: return "InvalidTopicCode";
            case MessageSetSizeTooLargeCode: return "MessageSetSizeTooLargeCode";
            case NotEnoughReplicasCode: return "NotEnoughReplicasCode";
            case NotEnoughReplicasAfterAppendCode: return "NotEnoughReplicasAfterAppendCode";
            case InvalidRequiredAcks: return "InvalidRequiredAcks";
            case IllegalConsumerGeneration: return "IllegalConsumerGeneration";
            case NoOffsetsCommittedCode: return "NoOffsetsCommittedCode";
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