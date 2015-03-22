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
            InvalidFetchSizeCode = 4,       // The message has a negative size
            LeaderNotAvailable = 5,         // This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
            NotLeaderForPartition = 6,      // This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition.It indicates that the clients metadata is out of date.
            RequestTimedOut = 7,            // This error is thrown if the request exceeds the user-specified time limit in the request.
            BrokerNotAvailable = 8,         // This is not a client facing error and is used only internally by intra-cluster broker communication.
            ReplicaNotAvailable = 9,        // If replica is expected on a broker, but is not.
            MessageSizeTooLarge = 10,       // The server has a configurable maximum message size to avoid unbounded memory allocation.This error is thrown if the client attempt to produce a message larger than this maximum.
            StaleControllerEpochCode = 11,  // Internal error code for broker-to-broker communication.
            OffsetMetadataTooLargeCode = 12,// If you specify a string larger than configured maximum for offset metadata
            StaleLeaderEpochCode = 13,
            OffsetsLoadInProgressCode = 14, // The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
            ConsumerCoordinatorNotAvailableCode = 15, // The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
            NotCoordinatorForConsumerCode = 16, // The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
            InvalidTopicCode = 17,
            MessageSetSizeTooLargeCode = 18,
            NotEnoughReplicasCode = 19,
            NotEnoughReplicasAfterAppendCode = 20,
            InvalidRequiredAcks = 21,  // used??
            IllegalConsumerGeneration = 22, // used??
            NoOffsetsCommittedCode = 23
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