package aidocs.doc_relay

import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals

class RelayPropertiesTest {

	@Test
	fun `defaults match the spec`() {
		val properties = RelayProperties()
		assertEquals(100, properties.drain.batchSize)
		assertEquals(Duration.ofSeconds(10), properties.polling.interval)
		assertEquals(Duration.ofSeconds(10), properties.backoff.base)
		assertEquals(Duration.ofMinutes(5), properties.backoff.max)
		assertEquals(5, properties.backoff.maxAttempts)
		assertEquals(Duration.ofMinutes(10), properties.dead.recoveryDelay)
		assertEquals(Duration.ofMinutes(5), properties.dead.recoveryScanInterval)
		assertEquals(Duration.ofMinutes(5), properties.zombie.lockTimeout)
		assertEquals(Duration.ofMinutes(1), properties.zombie.scanInterval)
		assertEquals(true, properties.listener.enabled)
		assertEquals("outbox_event", properties.listener.channel)
		assertEquals("doc.events.v1", properties.kafka.topic)
		assertEquals(3, properties.kafka.partitions)
		assertEquals(java.time.Duration.ofSeconds(10), properties.kafka.producer.maxBlock)
		assertEquals(java.time.Duration.ofSeconds(30), properties.kafka.producer.requestTimeout)
		assertEquals(java.time.Duration.ofSeconds(120), properties.kafka.producer.deliveryTimeout)
		assertEquals(1_048_576, properties.kafka.producer.maxRequestSize)
		assertEquals(java.time.Duration.ofSeconds(30), properties.shutdown.drainTimeout)
		assertEquals("", properties.admin.token)
	}
}
