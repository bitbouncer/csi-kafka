#include <boost/endian/arithmetic.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/crc.hpp>

#include "protocol_encoder.h"

namespace csi {
  namespace kafka {
    static const std::string client_id = "csi-kafka-v0.1";

    namespace internal
    {
      inline void encode_i08(boost::iostreams::stream<boost::iostreams::array_sink>& stream, int8_t val) {
        stream.write((const char*) &val, 1);
      }

      inline void encode_i16(boost::iostreams::stream<boost::iostreams::array_sink>& stream, int16_t val) {
        boost::endian::big_int16_t bev(val);
        stream.write((const char*) &bev, 2);
      }

      inline void encode_i32(boost::iostreams::stream<boost::iostreams::array_sink>& stream, int32_t val) {
        boost::endian::big_int32_t bev(val);
        stream.write((const char*) &bev, 4);
      }

      inline void encode_i64(boost::iostreams::stream<boost::iostreams::array_sink>& stream, int64_t val) {
        boost::endian::big_int64_t bev(val);
        stream.write((const char*) &bev, 8);
      }

      inline void encode_str(boost::iostreams::stream<boost::iostreams::array_sink>& stream, const std::string& s) {
        encode_i16(stream, (int16_t) s.size());
        stream.write(s.data(), s.size()); // value (without trailing NULL)
      }

      inline void encode_arr(boost::iostreams::stream<boost::iostreams::array_sink>& stream, const csi::kafka::basic_message::payload_type& value) {
        if(value.is_null())
          encode_i32(stream, (int32_t) -1);
        else {
          size_t len = value.size();
          encode_i32(stream, (int32_t) len);
          if(len)
            stream.write((const char*) &value[0], len);
        }
      }

      class delayed_size {
      public:
        delayed_size(boost::iostreams::stream<boost::iostreams::array_sink>& stream) : _stream(stream), _start(_stream.tellp()) {
          encode_i32(_stream, 0); // reserve space
        }

        ~delayed_size() {
          std::streampos cursor = _stream.tellp();
          _stream.seekp(_start);
          size_t size = cursor - _start - 4; // size not including size field 
          encode_i32(_stream, (int32_t) size);
          _stream.seekp(cursor);
        }

      private:
        boost::iostreams::stream<boost::iostreams::array_sink>& _stream;
        std::streampos                                          _start;
      };

      class delayed_crc {
      public:
        delayed_crc(boost::iostreams::stream<boost::iostreams::array_sink>& stream, const char* buffer) : _stream(stream), _buffer(buffer), _start(_stream.tellp()) {
          encode_i32(_stream, 0); // reserve space
        }

        ~delayed_crc() {
          std::streampos cursor = _stream.tellp();
          boost::crc_32_type result;
          result.process_bytes(_buffer + _start + std::streamoff(4), cursor - _start - 4);
          uint32_t crc = result.checksum();

          _stream.seekp(_start);
          encode_i32(_stream, *(int32_t*) &crc);
          _stream.seekp(cursor);
        }

      private:
        boost::iostreams::stream<boost::iostreams::array_sink>& _stream;
        const char*                                             _buffer;
        std::streampos                                          _start;
      };
    }


    size_t encode_produce_request(const std::string& topic, int partition, int required_acks, int timeout, const std::vector<std::shared_ptr<basic_message>>& v, int32_t correlation_id, char* buffer, size_t capacity) {
      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size total_message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::ProduceRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV0);
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);

        internal::encode_i16(ostr, required_acks);
        internal::encode_i32(ostr, timeout);

        internal::encode_i32(ostr, 1); // array size of topic data
        {
          internal::encode_str(ostr, topic);
          internal::encode_i32(ostr, 1); // array size of partitions
          {
            internal::encode_i32(ostr, partition);
            {
              internal::delayed_size message_set_size(ostr);

              // N.B., MessageSets are not preceded by an int32 like other array elements in the protocol.
              for(std::vector<std::shared_ptr<basic_message>>::const_iterator i = v.begin(); i != v.end(); ++i) {
                internal::encode_i64(ostr, 0); // offset (not known)
                internal::delayed_size message_set_size(ostr);
                internal::delayed_crc  message_set_crc(ostr, buffer);
                internal::encode_i08(ostr, 0); // magic byte
                internal::encode_i08(ostr, 0); // attributes
                internal::encode_arr(ostr, (*i)->key);
                internal::encode_arr(ostr, (*i)->value);
              }
            } // here is the message set size written
          } // end of partitions
        } // end of topic
      } // total_message_size written here
      return ostr.tellp();
    }

    //MetadataRequest = >[TopicName]
    //TopicName = > string
    size_t encode_metadata_request(const std::vector<std::string>& topics, int32_t correlation_id, char* buffer, size_t capacity) {
      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::MetadataRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV0);
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);

        internal::encode_i32(ostr, (int32_t) topics.size()); // number of topics
        for(std::vector<std::string>::const_iterator i = topics.begin(); i != topics.end(); ++i)
          internal::encode_str(ostr, *i);
      }
      return ostr.tellp();
    }

    size_t encode_simple_fetch_request(const std::string& topic, int32_t partition_id, int64_t fetch_offset, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id, char* buffer, size_t capacity) {
      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::FetchRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV0);
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);

        internal::encode_i32(ostr, -1); // should be -1 for clients
        internal::encode_i32(ostr, max_wait_time);
        internal::encode_i32(ostr, (int32_t) min_bytes);
        internal::encode_i32(ostr, 1); // nr of topics
        internal::encode_str(ostr, topic);
        internal::encode_i32(ostr, 1); // nr of partitions
        internal::encode_i32(ostr, partition_id);
        internal::encode_i64(ostr, fetch_offset);

        assert(capacity > 256);
        internal::encode_i32(ostr, (int32_t) (capacity - 100)); // estimated size of rest of reply without all the other jadda jadda..
      }
      return ostr.tellp();
    }

    size_t encode_multi_fetch_request(const std::string& topic, const std::vector<partition_cursor>& cursors, uint32_t max_wait_time, size_t min_bytes, int32_t correlation_id, char* buffer, size_t capacity) {
      int32_t max_bytes_per_partition = (int32_t) ((capacity - 100) / cursors.size());
      assert(capacity > 256);

      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::FetchRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV0);
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);

        internal::encode_i32(ostr, -1); // should be -1 for clients
        internal::encode_i32(ostr, max_wait_time);
        internal::encode_i32(ostr, (int32_t) min_bytes);
        internal::encode_i32(ostr, 1); // nr of topics
        internal::encode_str(ostr, topic);
        internal::encode_i32(ostr, (int32_t) cursors.size()); // nr of partitions

        for(std::vector<partition_cursor>::const_iterator i = cursors.begin(); i != cursors.end(); ++i) {
          internal::encode_i32(ostr, i->_partition_id);
          internal::encode_i64(ostr, i->_next_offset);
          internal::encode_i32(ostr, max_bytes_per_partition); // estimated size of rest of reply without all the other jadda jadda..
        }
      }
      return ostr.tellp();
    }


    size_t encode_simple_offset_request(const std::string& topic, int32_t partition_id, int64_t time, int32_t max_number_of_offsets, int32_t correlation_id, char* buffer, size_t capacity) {
      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::OffsetRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV0);  // TBD VERSION1 
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);

        internal::encode_i32(ostr, -1); // replica id for clients
        internal::encode_i32(ostr, 1); // nr of topics
        internal::encode_str(ostr, topic);
        internal::encode_i32(ostr, 1); // nr of partitions
        internal::encode_i32(ostr, partition_id);
        internal::encode_i64(ostr, time);
        internal::encode_i32(ostr, max_number_of_offsets);
      }
      return ostr.tellp();
    }

    //Group Coordinator Request (aka Cluster Metadata Request)
    size_t encode_group_coordinator_request(const std::string& consumer_group, int32_t correlation_id, char* buffer, size_t capacity) {
      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::GroupCoordinatorRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV0);
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);
        internal::encode_str(ostr, consumer_group);
      }
      return ostr.tellp();
    }

    /*
    v0 (supported in 0.8.1 or later)
    OffsetCommitRequest => ConsumerGroup [TopicName [Partition Offset Metadata]]
    ConsumerGroup => string
    TopicName => string
    Partition => int32
    Offset => int64
    Metadata => string

    v1 (supported in 0.8.2 or later)
    OffsetCommitRequest => ConsumerGroupId ConsumerGroupGenerationId ConsumerId [TopicName [Partition Offset TimeStamp Metadata]]
    ConsumerGroupId => string
    ConsumerGroupGenerationId => int32
    ConsumerId => string
    TopicName => string
    Partition => int32
    Offset => int64
    TimeStamp => int64
    Metadata => string

    */
    //Offset Commit Request
    size_t encode_simple_offset_commit_request(
      const std::string& ConsumerGroupId,
      int32_t ConsumerGroupGenerationId,
      const std::string& ConsumerId,
      const std::string& topic,
      const std::vector<topic_offset>& offsets,
      const std::string& metadata,
      int32_t correlation_id,
      char* buffer,
      size_t capacity) {
      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::OffsetCommitRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV1);
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);
        internal::encode_str(ostr, ConsumerGroupId);
        internal::encode_i32(ostr, ConsumerGroupGenerationId);
        internal::encode_str(ostr, ConsumerId);
        internal::encode_i32(ostr, 1); // nr of topics
        internal::encode_str(ostr, topic);
        internal::encode_i32(ostr, (int32_t) offsets.size()); // nr of partitions
        for(std::vector<topic_offset>::const_iterator i = offsets.begin(); i != offsets.end(); ++i) {
          internal::encode_i32(ostr, i->partition);
          internal::encode_i64(ostr, i->offset);
          internal::encode_i64(ostr, 0); // next version will change again to require 0 and have an added retention. lets prepare for that
          internal::encode_str(ostr, metadata);
        }
      }
      return ostr.tellp();
    }

    size_t encode_simple_offset_commit_request(
      const std::string& ConsumerGroupId,
      int32_t ConsumerGroupGenerationId,
      const std::string& ConsumerId,
      const std::string& topic,
      const std::map<int32_t, int64_t>& offsets,
      const std::string& metadata,
      int32_t correlation_id,
      char* buffer,
      size_t capacity) {
      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::OffsetCommitRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV1);
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);
        internal::encode_str(ostr, ConsumerGroupId);
        internal::encode_i32(ostr, ConsumerGroupGenerationId);
        internal::encode_str(ostr, ConsumerId);
        internal::encode_i32(ostr, 1); // nr of topics
        internal::encode_str(ostr, topic);
        internal::encode_i32(ostr, (int32_t) offsets.size()); // nr of partitions
        for(std::map<int32_t, int64_t>::const_iterator i = offsets.begin(); i != offsets.end(); ++i) {
          internal::encode_i32(ostr, i->first);
          internal::encode_i64(ostr, i->second);
          internal::encode_i64(ostr, 0); // next version will change again to require 0 and have an added retention. lets prepare for that
          internal::encode_str(ostr, metadata);
        }
      }
      return ostr.tellp();
    }

    /*
    OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
    */
    //Offset Fetch Request
    size_t encode_simple_offset_fetch_request(const std::string& consumer_group, const std::string& topic, int32_t partition_id, int32_t correlation_id, char* buffer, size_t capacity) {
      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::OffsetFetchRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV1); // V0 reads from zookeeper V1 from topic.... SAME API
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);
        internal::encode_str(ostr, consumer_group);
        internal::encode_i32(ostr, 1); // nr of topics
        internal::encode_str(ostr, topic);
        internal::encode_i32(ostr, 1); // nr of partitions
        internal::encode_i32(ostr, partition_id);
      }
      return ostr.tellp();
    }

    size_t encode_simple_offset_fetch_request(const std::string& consumer_group, const std::string& topic, int32_t correlation_id, char* buffer, size_t capacity) {
      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::OffsetFetchRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV1); // V0 reads from zookeeper V1 from topic.... SAME API
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);
        internal::encode_str(ostr, consumer_group);
        internal::encode_i32(ostr, 1); // nr of topics
        internal::encode_str(ostr, topic);
        internal::encode_i32(ostr, 0); // nr of partitions
      }
      return ostr.tellp();
    }


    //Offset Fetch Request
    size_t encode_offset_fetch_all_request(const std::string& consumer_group, int32_t correlation_id, char* buffer, size_t capacity) {
      boost::iostreams::stream<boost::iostreams::array_sink> ostr(buffer, capacity);
      {
        internal::delayed_size message_size(ostr);
        internal::encode_i16(ostr, csi::kafka::OffsetFetchRequest);
        internal::encode_i16(ostr, csi::kafka::ApiVersionV1);
        internal::encode_i32(ostr, correlation_id);
        internal::encode_str(ostr, client_id);
        internal::encode_str(ostr, consumer_group);
        internal::encode_i32(ostr, 0); // nr of topics
        //internal::encode_str(ostr, topic);
        //internal::encode_i32(ostr, 1); // nr of partitions
        //internal::encode_i32(ostr, partition_id);
      }
      return ostr.tellp();
    }

  }
}