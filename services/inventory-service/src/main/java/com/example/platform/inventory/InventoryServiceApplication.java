package com.example.platform.inventory;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.converter.StringJacksonJsonMessageConverter;

@SpringBootApplication
public class InventoryServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(InventoryServiceApplication.class, args);
	}

	// Boot plugs this into every @KafkaListener: the record value arrives as a String
	// and is converted to the listener method's parameter type (EventEnvelope<OrderCreated>).
	@Bean
	StringJacksonJsonMessageConverter messageConverter() {
		return new StringJacksonJsonMessageConverter();
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
