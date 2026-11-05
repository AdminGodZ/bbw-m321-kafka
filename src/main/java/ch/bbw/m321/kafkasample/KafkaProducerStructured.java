package ch.bbw.m321.kafkasample;

import ch.bbw.m321.kafkasample.dto.StructuredMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaProducerStructured {

	public static final String TOPIC = "structuredTopic";

	private final KafkaTemplate<String, StructuredMessage> kafkaTemplate;

	public void sendMessage(StructuredMessage message) {
		kafkaTemplate.send(TOPIC, message);
	}
}
