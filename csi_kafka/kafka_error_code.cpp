#ifdef WIN32 
#define BOOST_ASIO_ERROR_CATEGORY_NOEXCEPT noexcept(true)
#endif

#include "kafka_error_code.h"

namespace csi {
  namespace kafka {
    std::string to_string(error_codes error) {
      switch(error) {
      case Unknown: return "Unknown (An unexpected server error)"; // -1
      case NoError: return "NoError (OK)";                         // 0
      case OffsetOutOfRange: return "OffsetOutOfRange";
      case InvalidMessage: return "InvalidMessage (Bad CRC)";
      case UnknownTopicOrPartition: return "UnknownTopicOrPartition";
      case InvalidMessageSize: return "InvalidMessageSize";
      case LeaderNotAvailable: return "LeaderNotAvailable";
      case NotLeaderForPartition: return "NotLeaderForPartition";
      case RequestTimedOut: return "RequestTimedOut";
      case BrokerNotAvailable: return "BrokerNotAvailable";
      case ReplicaNotAvailable: return "ReplicaNotAvailable";
      case MessageSizeTooLarge: return "MessageSizeTooLarge";
      case StaleControllerEpoch: return "StaleControllerEpoch";
      case OffsetMetadataTooLarge: return "OffsetMetadataTooLarge";
      case NetworkException: return "NetworkException";
      case GroupLoadInProgress: return "GroupLoadInProgressCode";
      case GroupCoordinatorNotAvailable: return "GroupCoordinatorNotAvailable";
      case NotCoordinatorForGroup: return "NotCoordinatorForGroup";
      case InvalidTopic: return "InvalidTopic";
      case RecordListTooLarge: return "RecordListTooLarge";
      case NotEnoughReplicas: return "NotEnoughReplicas";
      case NotEnoughReplicasAfterAppend: return "NotEnoughReplicasAfterAppend";
      case InvalidRequiredAcks: return "InvalidRequiredAcks";
      case IllegalGeneration: return "IllegalGeneration";
      case InconsistentGroupProtocol: return "InconsistentGroupProtocol";
      case InvalidGroupId: return "InvalidGroupId";
      case UnknownMemberId: return "UnknownMemberId";
      case InvalidSessionTimeout: return "InvalidSessionTimeout";
      case RebalanceInProgress: return "RebalanceInProgress";
      case InvalidCommitOffsetSize: return "InvalidCommitOffsetSize";
      case TopicAuthorizationFailed: return "TopicAuthorizationFailed";
      case GroupAuthorizationFailed: return "GroupAuthorizationFailed";
      case ClusterAuthorizationFailed: return "ClusterAuthorizationFailed";
      default:
      return "Undefined error #" + std::to_string((int) error);
      }
    }

    std::string to_string(const rpc_error_code& e) {
      return std::string("system:") + e.ec1.message() + ", kafka:" + to_string(e.ec2);
    }

  };
};