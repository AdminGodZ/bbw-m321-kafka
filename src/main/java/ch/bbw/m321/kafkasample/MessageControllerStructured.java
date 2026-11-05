package ch.bbw.m321.kafkasample;

import ch.bbw.m321.kafkasample.dto.StructuredMessage;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class MessageControllerStructured {

	private final KafkaProducerStructured producer;

	@PostMapping("/sendStructured")
	public StructuredMessage sendMessage(@RequestParam("message") String message) {
		var msg = new StructuredMessage(UUID.randomUUID(), message, LocalDateTime.now(ZoneId.systemDefault()));
		producer.sendMessage(msg);
		return msg;
	}
}
