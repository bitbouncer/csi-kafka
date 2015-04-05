SET(LIB_SRCS
	avro_decoder.h
	avro_lowlevel_producer.h
    avro_highlevel_producer
	internal/async.h
    internal/async_metadata_client.h
    internal/async_metadata_client.cpp
    internal/call_context.h
    internal/utility.h
	internal/spinlock.cpp
	internal/spinlock.h
    lowlevel_client.h
	lowlevel_client.cpp
	protocol_encoder.h
	protocol_encoder.cpp
	protocol_decoder.h
	protocol_decoder.cpp
	lowlevel_consumer.cpp
	lowlevel_consumer.h
	lowlevel_producer.cpp
	lowlevel_producer.h
    highlevel_consumer.h
    highlevel_consumer.cpp
    highlevel_producer.h
    highlevel_producer.cpp
    async_lowlevel_producer.h
    async_lowlevel_producer.cpp
	kafka.h
	kafka.cpp
    kafka_error_code.h
    kafka_error_code.cpp
	)
	
add_library (csi-kafka STATIC ${LIB_SRCS})

