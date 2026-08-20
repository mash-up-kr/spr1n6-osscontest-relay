package aidocs.doc_relay.outbox

import aidocs.doc_relay.support.RelayIntegrationTest
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.ProducerFactory
import kotlin.test.assertEquals

class KafkaProducerConfigTest : RelayIntegrationTest() {

	@Autowired
	private lateinit var producerFactory: ProducerFactory<String, ByteArray>

	@Test
	fun `explicitly sets producer timeouts and message size cap`() {
		val configs = producerFactory.configurationProperties

		assertEquals(10_000, configs[ProducerConfig.MAX_BLOCK_MS_CONFIG])
		assertEquals(30_000, configs[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG])
		assertEquals(120_000, configs[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG])
		assertEquals(1_048_576, configs[ProducerConfig.MAX_REQUEST_SIZE_CONFIG])
	}
}
