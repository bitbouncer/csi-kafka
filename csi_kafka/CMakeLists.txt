SET(LIB_SRCS
	avro/decoder.h
	avro/avro_producer.h
	internal/async_metadata_client.h
    internal/async_metadata_client.cpp
    internal/call_context.h
    low_level/client.h
	low_level/client.cpp
	low_level/encoder.h
	low_level/encoder.cpp
	low_level/decoder.h
	low_level/decoder.cpp
	low_level/spinlock.cpp
	low_level/spinlock.h
	low_level/utility.h
	low_level/consumer.cpp
	low_level/consumer.h
	low_level/producer.cpp
	low_level/producer.h
    high_level_consumer.h
    high_level_consumer.cpp
    high_level_producer.h
    high_level_producer.cpp
	kafka.h
	kafka.cpp
    kafka_error_code.h
    kafka_error_code.cpp
	)
	
add_library (csi_kafka STATIC ${LIB_SRCS})

