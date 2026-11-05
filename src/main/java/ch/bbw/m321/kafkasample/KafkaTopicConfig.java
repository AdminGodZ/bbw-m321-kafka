package ch.bbw.m321.kafkasample;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Creates the topics on startup, so the listeners can subscribe before the first message is sent.
 */
@Configuration
public class KafkaTopicConfig {

	@Bean
	NewTopic testTopic() {
		return TopicBuilder.name(KafkaProducer.TOPIC).build();
	}

	@Bean
	NewTopic structuredTopic() {
		return TopicBuilder.name(KafkaProducerStructured.TOPIC).build();
	}

	@Bean
	NewTopic transactionTopic() {
		return TopicBuilder.name(KafkaTransactionalProducer.TOPIC).build();
	}
}
