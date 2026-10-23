package com.example.platform.inventory;

import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;

import java.time.Duration;

@Configuration
@EnableCaching
class CacheConfig {

	// Redis cache for GET /stock/{sku}.
	// - TTL: even if an eviction is missed, a stale value lives at most 10 minutes.
	// - JSON values: readable with redis-cli, no Java class names stored.
	// - transactionAware: evict() inside a DB transaction runs only AFTER that transaction commits.
	@Bean
	RedisCacheManager cacheManager(RedisConnectionFactory connectionFactory) {
		var config = RedisCacheConfiguration.defaultCacheConfig()
				.entryTtl(Duration.ofMinutes(10))
				.serializeValuesWith(SerializationPair.fromSerializer(new JacksonJsonRedisSerializer<>(StockService.StockResponse.class)));
		return RedisCacheManager.builder(connectionFactory).cacheDefaults(config).transactionAware().build();
	}
}
