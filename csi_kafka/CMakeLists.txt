SET(LIB_SRCS
	avro/avro_consumer.h
	avro/avro_producer.h
	low_level/client.h
	low_level/client.cpp
	low_level/encoder.h
	low_level/encoder.cpp
	low_level/decoder.h
	low_level/decoder.cpp
	low_level/spinlock.cpp
	low_level/spinlock.h
	low_level/utility.h
	consumer.cpp
	consumer.h
	kafka.h
	kafka.cpp
	producer.cpp
	producer.h
	)
	
add_library (csi_kafka STATIC ${LIB_SRCS})

