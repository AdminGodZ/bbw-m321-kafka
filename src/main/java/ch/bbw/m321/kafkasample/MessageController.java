package ch.bbw.m321.kafkasample;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class MessageController {

	private final KafkaProducer producer;

	@PostMapping("/send")
	public String sendMessage(@RequestParam("message") String message) {
		producer.sendMessage(message);
		return "Message sent";
	}
}
