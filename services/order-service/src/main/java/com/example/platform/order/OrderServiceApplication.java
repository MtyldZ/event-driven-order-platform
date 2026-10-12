package com.example.platform.order;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class OrderServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderServiceApplication.class, args);
	}

	// KafkaAdmin creates this topic on startup if it does not exist yet.
	@Bean
	NewTopic orderCreatedTopic() {
		return TopicBuilder.name(OrderService.ORDER_CREATED).partitions(3).replicas(1).build();
	}

}
