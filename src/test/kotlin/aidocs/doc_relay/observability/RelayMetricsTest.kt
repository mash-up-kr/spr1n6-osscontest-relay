package aidocs.doc_relay.observability

import aidocs.doc_relay.outbox.OutboxDrainer
import aidocs.doc_relay.support.RelayIntegrationTest
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestPropertySource(
	properties = [
		"relay.polling.interval=1h",
		"relay.zombie.scan-interval=1h",
		"relay.dead.recovery-scan-interval=1h",
		"relay.metrics.gauge-refresh-interval=1h",
	]
)
class RelayMetricsTest : RelayIntegrationTest() {

	@Autowired private lateinit var registry: MeterRegistry
	@Autowired private lateinit var metrics: RelayMetrics
	@Autowired private lateinit var drainer: OutboxDrainer

	@Test
	fun `counts published events`() {
		val documentId = seedParents()
		insertVersion(documentId)
		val before = registry.counter("relay.publish.total", "result", "success").count()

		drainer.drainOnce()

		assertEquals(before + 1, registry.counter("relay.publish.total", "result", "success").count())
	}

	@Test
	fun `records publish latency from created_at`() {
		val documentId = seedParents()
		insertVersion(documentId)

		drainer.drainOnce()

		assertTrue(registry.timer("relay.publish.latency").count() >= 1)
	}

	@Test
	fun `gauges separate active dead rows from held ones`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		insertOutbox(documentId, versionId, status = "DEAD",
			nextAttemptAt = Instant.now().minus(1, ChronoUnit.MINUTES))
		val heldId = insertOutbox(documentId, versionId, status = "DEAD")
		jdbc.sql("UPDATE outbox_event SET next_attempt_at = 'infinity' WHERE id = :id")
			.param("id", heldId).update()

		metrics.refreshGauges()

		assertEquals(1.0, registry.get("relay.outbox.dead").gauge().value())
		assertEquals(1.0, registry.get("relay.outbox.held").gauge().value())
	}
}
