#include "kafka.h"


namespace csi
{
    namespace kafka
    {
        std::string to_string(error_codes error)
        {
            switch (error)
            {
            case NoError: return "NoError";
            case Unknown: return "Unknown (An unexpected server error)";
            case OffsetOutOfRange: return "OffsetOutOfRange";
            case InvalidMessage: return "InvalidMessage (Bad CRC)";
            case UnknownTopicOrPartition: return "UnknownTopicOrPartition";
            case InvalidMessageSize: return "InvalidMessageSize";
            case LeaderNotAvailable: return "LeaderNotAvailable";
            case NotLeaderForPartition: return "NotLeaderForPartition";
            case RequestTimedOut: return "RequestTimedOut";
            case BrokerNotAvailable: return "BrokerNotAvailable";
            case Unused: return "Unused error code";
            case MessageSizeTooLarge: return "MessageSizeTooLarge";
            case StaleControllerEpochCode: return "StaleControllerEpochCode";
            case OffsetMetadataTooLargeCode: return "OffsetMetadataTooLargeCode";
            default:
                return "Undefined error #" + std::to_string((int) error);
            }

        }
    };
};