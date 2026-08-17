package aidocs.doc_relay

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import kotlin.test.assertEquals

/**
 * Proves that `application.yaml` keys actually bind to `RelayProperties` via Spring's
 * relaxed @ConfigurationProperties binding (kebab-case → camelCase). The defaults-only
 * test cannot catch YAML typos because every Kotlin default equals its YAML value —
 * e.g. a misspelled key like `bacth-size` silently falls back to the identical default.
 * This test catches such typos by asserting the real Spring context bean values.
 *
 * NOTE: RelayIntegrationTest's @TestPropertySource overrides five properties to 1h/false.
 * We assert only the eleven properties NOT overridden, which prove the YAML binding path.
 */
class RelayPropertiesBindingTest : RelayIntegrationTest() {

	@Autowired
	private lateinit var properties: RelayProperties

	@Test
	fun `application yaml binds to relay properties`() {
		// drain — not overridden
		assertEquals(100, properties.drain.batchSize)

		// backoff — not overridden
		assertEquals(Duration.ofSeconds(10), properties.backoff.base)
		assertEquals(Duration.ofMinutes(5), properties.backoff.max)
		assertEquals(5, properties.backoff.maxAttempts)

		// dead.recoveryDelay — not overridden (only recoveryScanInterval is)
		assertEquals(Duration.ofMinutes(10), properties.dead.recoveryDelay)

		// zombie.lockTimeout — not overridden (only scanInterval is)
		assertEquals(Duration.ofMinutes(5), properties.zombie.lockTimeout)

		// listener channel and reconnect settings — not overridden (only enabled is)
		assertEquals("outbox_event", properties.listener.channel)
		assertEquals(Duration.ofSeconds(1), properties.listener.reconnectBase)
		assertEquals(Duration.ofSeconds(30), properties.listener.reconnectMax)

		// kafka — not overridden
		assertEquals("doc.events.v1", properties.kafka.topic)
		assertEquals(3, properties.kafka.partitions)
	}
}
