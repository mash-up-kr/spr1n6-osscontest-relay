package aidocs.doc_relay.outbox

import aidocs.doc_relay.support.RelayIntegrationTest
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 브로커 크기 상한 초과 같은 영구 실패는 재시도 없이 1회 만에 DEAD + 정지(held) 상태로 간다. */
class FailureClassificationDrainTest : RelayIntegrationTest() {

	@Autowired private lateinit var drainer: OutboxDrainer
	@Autowired private lateinit var registry: MeterRegistry

	@Test
	fun `a message over the broker limit goes dead on the first attempt without retrying`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		val id = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId).query(UUID::class.java).single()
		jdbc.sql("UPDATE outbox_event SET payload = jsonb_build_object('pad', repeat('a', 1200000)) WHERE id = :id")
			.param("id", id).update()
		val before = registry.counter("relay.dead.transition.total").count()

		drainer.drainOnce()

		assertEquals("DEAD", statusOf(id))
		assertEquals(
			1,
			jdbc.sql("SELECT publish_attempt_count FROM outbox_event WHERE id = :id")
				.param("id", id).query(Int::class.java).single(),
			"영구 실패는 재시도 없이 1회 만에 DEAD 여야 한다",
		)
		assertTrue(
			jdbc.sql("SELECT next_attempt_at = 'infinity' FROM outbox_event WHERE id = :id")
				.param("id", id).query(Boolean::class.java).single(),
			"영구 실패는 곧바로 정지된 DEAD(held) 여야 한다",
		)
		assertEquals(before + 1, registry.counter("relay.dead.transition.total").count())
	}
}
