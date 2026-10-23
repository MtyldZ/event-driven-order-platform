package com.example.platform.inventory;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.converter.StringJacksonJsonMessageConverter;

import java.util.Map;

// Kafka beans live here, not on the @SpringBootApplication class: test slices (@WebMvcTest, @DataJpaTest)
// always load the main class, but leave other @Configuration classes out.
@Configuration
class KafkaConfig {

	// Record value arrives as a String and is converted to the @KafkaListener parameter type.
	@Bean
	StringJacksonJsonMessageConverter messageConverter() {
		return new StringJacksonJsonMessageConverter();
	}

	// When a listener throws: retry the record 3 times (1s, 2s, 4s), then publish it unchanged to <topic>.DLT
	// and move on. Conversion errors (malformed JSON) skip the retries: they would fail the same way every time.
	@Bean
	DefaultErrorHandler kafkaErrorHandler(ProducerFactory<Object, Object> producerFactory) {
		// Values arrive as raw Strings; re-serializing them as JSON would wrap them in quotes.
		var dltTemplate = new KafkaTemplate<>(producerFactory,
				Map.<String, Object>of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
		var backOff = new ExponentialBackOffWithMaxRetries(3);
		backOff.setInitialInterval(1_000);
		backOff.setMultiplier(2);
		// Spring Kafka 4 defaults to "<topic>-dlt"; our contract says "<topic>.DLT", same partition as the original.
		var recoverer = new DeadLetterPublishingRecoverer(dltTemplate,
				(record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
		return new DefaultErrorHandler(recoverer, backOff);
	}

	@Bean
	NewTopic inventoryReservedTopic() {
		return TopicBuilder.name(Events.INVENTORY_RESERVED).partitions(3).replicas(1).build();
	}

	@Bean
	NewTopic inventoryRejectedTopic() {
		return TopicBuilder.name(Events.INVENTORY_REJECTED).partitions(3).replicas(1).build();
	}

}
