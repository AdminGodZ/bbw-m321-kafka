package ch.bbw.m321.kafkasample;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class KafkaTransactionalProducer {

	public static final String TOPIC = "transactionTopic";

	private final KafkaTemplate<String, String> kafkaTemplate;

	@Transactional
	public void sendTwo(String first, String second, boolean failBetween) {
		kafkaTemplate.send(TOPIC, first);
		if (failBetween) {
			// send() is async: flush so the first message really reaches the broker before the abort
			kafkaTemplate.flush();
			throw new IllegalStateException("Simulated failure between the two messages");
		}
		kafkaTemplate.send(TOPIC, second);
	}
}
