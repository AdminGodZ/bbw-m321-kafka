package ch.bbw.m321.kafkasample;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class KafkaTransactionConfig {

	/**
	 * With a `transaction-id-prefix` every send must run inside a transaction.
	 * The earlier chapters send without one, so we explicitly allow that.
	 */
	@Bean
	SmartInitializingSingleton allowNonTransactionalSends(KafkaTemplate<?, ?> kafkaTemplate) {
		return () -> kafkaTemplate.setAllowNonTransactional(true);
	}
}
