package ch.bbw.m321.kafkasample;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaTransactionalConsumer {

	@KafkaListener(topics = KafkaTransactionalProducer.TOPIC, groupId = "myGroup")
	public void listen(String message) {
		log.info("Received committed message: {}", message);
	}
}
