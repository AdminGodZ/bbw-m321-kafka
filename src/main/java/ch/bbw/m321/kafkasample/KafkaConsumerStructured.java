package ch.bbw.m321.kafkasample;

import ch.bbw.m321.kafkasample.dto.StructuredMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumerStructured {

	@KafkaListener(topics = KafkaProducerStructured.TOPIC, groupId = "myGroup")
	public void listen(StructuredMessage message) {
		log.info("YAY Received message in group myGroup: {}", message);
	}
}
