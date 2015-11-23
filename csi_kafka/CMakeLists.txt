SET(LIB_SRCS
	internal/async.h
    internal/async_metadata_client.h
    internal/async_metadata_client.cpp
    internal/call_context.h
    internal/utility.h
	internal/spinlock.cpp
	internal/spinlock.h
    consumer_coordinator.h
    consumer_coordinator.cpp
    lowlevel_client.h
	lowlevel_client.cpp
	protocol_encoder.h
	protocol_encoder.cpp
	protocol_decoder.h
	protocol_decoder.cpp
	lowlevel_consumer.cpp
	lowlevel_consumer.h
    highlevel_consumer.h
    highlevel_consumer.cpp
    lowlevel_producer.h
    lowlevel_producer.cpp
    highlevel_producer.h
    highlevel_producer.cpp
	kafka.h
	kafka.cpp
    kafka_error_code.h
    kafka_error_code.cpp
	)
	
add_library (csi-kafka STATIC ${LIB_SRCS})

