package aidocs.doc_relay.signal

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DrainTriggerTest : RelayIntegrationTest() {

	@Autowired private lateinit var trigger: DrainTrigger

	@Test
	fun `a signal drains the pending row`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		val id = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId).query(UUID::class.java).single()

		trigger.signal()

		assertTrue(trigger.awaitIdle(20_000), "드레인이 끝나지 않았다")
		assertEquals("PUBLISHED", statusOf(id))
	}

	@Test
	fun `a thousand signals collapse but nothing is left behind`() {
		val documentId = seedParents()
		(1L..5L).forEach { insertVersion(documentId, versionNo = it) }

		repeat(1000) { trigger.signal() }

		assertTrue(trigger.awaitIdle(30_000), "드레인이 끝나지 않았다")
		assertEquals(
			0,
			jdbc.sql("SELECT count(*) FROM outbox_event WHERE status <> 'PUBLISHED'")
				.query(Int::class.java).single(),
		)
	}

	@Test
	fun `signal returns immediately`() {
		val started = System.currentTimeMillis()
		trigger.signal()
		assertTrue(System.currentTimeMillis() - started < 500, "signal 이 블로킹됐다")
	}
}
