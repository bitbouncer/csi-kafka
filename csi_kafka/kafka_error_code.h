#include <string>
#include <boost/system/system_error.hpp>

#pragma once

namespace csi
{
    namespace kafka
    {
        enum error_codes
        {
            Unknown = -1,                   // An unexpected server error
            NoError = 0,                    // No error--it worked!
            OffsetOutOfRange = 1,           // The requested offset is outside the range of offsets maintained by the server for the given topic / partition.
            InvalidMessage = 2,             // This indicates that a message contents does not match its CRC
            UnknownTopicOrPartition = 3,    // This request is for a topic or partition that does not exist on this broker.
            InvalidMessageSize = 4,         // The message has a negative size
            LeaderNotAvailable = 5,         // This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
            NotLeaderForPartition = 6,      // This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition.It indicates that the clients metadata is out of date.
            RequestTimedOut = 7,            // This error is thrown if the request exceeds the user-specified time limit in the request.
            BrokerNotAvailable = 8,         // This is not a client facing error and is used only internally by intra-cluster broker communication.
            ReplicaNotAvailable = 9,        // If replica is expected on a broker, but is not.
            MessageSizeTooLarge = 10,       // The server has a configurable maximum message size to avoid unbounded memory allocation.This error is thrown if the client attempt to produce a message larger than this maximum.
            StaleControllerEpoch = 11,  // Internal error code for broker-to-broker communication.
            OffsetMetadataTooLarge = 12,// If you specify a string larger than configured maximum for offset metadata
            NetworkException = 13,
            GroupLoadInProgress = 14,   // The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
            GroupCoordinatorNotAvailable = 15, // The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
            NotCoordinatorForGroup = 16, // The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
            InvalidTopic = 17,           // For a request which attempts to access an invalid topic (e.g. one which has an illegal name), or if an attempt is made to write to an internal topic (such as the consumer offsets topic).
            RecordListTooLarge = 18,     // If a message batch in a produce request exceeds the maximum configured segment size.
            NotEnoughReplicas = 19,      //Returned from a produce request when the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1.
            NotEnoughReplicasAfterAppend = 20, // Returned from a produce request when the message was written to the log, but with fewer in-sync replicas than required.
            InvalidRequiredAcks = 21,    // Returned from a produce request if the requested requiredAcks is invalid (anything other than -1, 1, or 0).
            IllegalGeneration = 22,      // used??
            InconsistentGroupProtocol = 23, // Returned in join group when the member provides a protocol type or set of protocols which is not compatible with the current group.
            InvalidGroupId = 24,
            UnknownMemberId = 25,
            InvalidSessionTimeout = 26,
            RebalanceInProgress = 27,
            InvalidCommitOffsetSize = 28,
            TopicAuthorizationFailed = 29,
            GroupAuthorizationFailed = 30,
            ClusterAuthorizationFailed = 31
        };

        std::string to_string(error_codes);

        struct rpc_error_code
        {
            inline rpc_error_code(const boost::system::error_code& e1 = make_error_code(boost::system::errc::success), csi::kafka::error_codes e2 = NoError) : ec1(e1), ec2(e2) {}

            inline operator bool() const  BOOST_SYSTEM_NOEXCEPT // true if error
            {
                return (ec1 || ec2 != 0);
            }

            inline bool operator!() const  BOOST_SYSTEM_NOEXCEPT // true if no error
            {
                return (!ec1 && ec2 == 0);
            }

            boost::system::error_code ec1;
            csi::kafka::error_codes   ec2;
        };

        std::string to_string(const rpc_error_code&);
    };
};