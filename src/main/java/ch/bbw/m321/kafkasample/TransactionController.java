package ch.bbw.m321.kafkasample;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class TransactionController {

	private final KafkaTransactionalProducer producer;

	@PostMapping("/sendTransactional")
	public String sendTransactional(@RequestParam String first, @RequestParam String second,
			@RequestParam(defaultValue = "false") boolean fail) {
		producer.sendTwo(first, second, fail);
		return "Both messages sent";
	}
}
