SET(LIB_SRCS
	avro/decoder.h
	avro/avro_producer.h
    avro/avro_high_level_producer
	internal/async.h
    internal/async_metadata_client.h
    internal/async_metadata_client.cpp
    internal/call_context.h
    internal/utility.h
	internal/spinlock.cpp
	internal/spinlock.h
    lowlevel_client.h
	lowlevel_client.cpp
	encoder.h
	encoder.cpp
	decoder.h
	decoder.cpp
	lowlevel_consumer.cpp
	lowlevel_consumer.h
	lowlevel_producer.cpp
	lowlevel_producer.h
    high_level_consumer.h
    high_level_consumer.cpp
    high_level_producer.h
    high_level_producer.cpp
    async_lowlevel_producer.h
    async_lowlevel_producer.cpp
	kafka.h
	kafka.cpp
    kafka_error_code.h
    kafka_error_code.cpp
	)
	
add_library (csi_kafka STATIC ${LIB_SRCS})

