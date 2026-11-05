package ch.bbw.m321.kafkasample;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import ch.bbw.m321.kafkasample.dto.StructuredMessage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

@SpringBootTest(properties = "spring.kafka.consumer.auto-offset-reset=earliest")
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers", brokerProperties = {
		"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1" })
class KafkasampleApplicationTests {

	private static final long TIMEOUT_MS = 15_000;

	@Autowired
	private MessageController messageController;

	@Autowired
	private MessageControllerStructured messageControllerStructured;

	@Autowired
	private KafkaTransactionalProducer transactionalProducer;

	@MockitoSpyBean
	private KafkaConsumer consumer;

	@MockitoSpyBean
	private KafkaConsumerStructured consumerStructured;

	@MockitoSpyBean
	private KafkaTransactionalConsumer transactionalConsumer;

	@Test
	void stringMessageIsConsumed() {
		messageController.sendMessage("Hello");

		verify(consumer, timeout(TIMEOUT_MS)).listen("Hello");
	}

	@Test
	void structuredMessageIsConsumedAsRecord() {
		var sent = messageControllerStructured.sendMessage("Hello Again");

		verify(consumerStructured, timeout(TIMEOUT_MS)).listen(argThat((StructuredMessage m) -> m.equals(sent)));
	}

	@Test
	void committedTransactionDeliversBothMessages() {
		transactionalProducer.sendTwo("tx-ok-1", "tx-ok-2", false);

		verify(transactionalConsumer, timeout(TIMEOUT_MS)).listen("tx-ok-1");
		verify(transactionalConsumer, timeout(TIMEOUT_MS)).listen("tx-ok-2");
	}

	@Test
	void abortedTransactionDeliversNothing() {
		assertThrows(IllegalStateException.class, () -> transactionalProducer.sendTwo("tx-fail-1", "tx-fail-2", true));
		// a later committed message proves the consumer has read past the aborted one
		transactionalProducer.sendTwo("tx-after-1", "tx-after-2", false);

		verify(transactionalConsumer, timeout(TIMEOUT_MS)).listen("tx-after-2");
		verify(transactionalConsumer, after(1_000).never()).listen("tx-fail-1");
		verify(transactionalConsumer, after(1_000).never()).listen("tx-fail-2");
	}
}
