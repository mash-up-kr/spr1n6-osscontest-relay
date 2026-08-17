package aidocs.doc_relay.outbox

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OutboxRepositoryMarkTest : RelayIntegrationTest() {

	@Autowired
	private lateinit var repository: OutboxRepository

	private fun freshPending(attemptCount: Int = 0): UUID {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		return insertOutbox(documentId, versionId, attemptCount = attemptCount)
	}

	private fun nextAttemptAt(id: UUID): Instant =
		jdbc.sql("SELECT next_attempt_at FROM outbox_event WHERE id = :id")
			.param("id", id).query(Timestamp::class.java).single().toInstant()

	private fun attemptCount(id: UUID): Int =
		jdbc.sql("SELECT publish_attempt_count FROM outbox_event WHERE id = :id")
			.param("id", id).query(Int::class.java).single()

	@Test
	fun `markPublished sets published_at and clears the lock`() {
		val id = freshPending()
		repository.claimBatch(10)

		repository.markPublished(listOf(id))

		assertEquals("PUBLISHED", statusOf(id))
		assertNotNull(
			jdbc.sql("SELECT published_at FROM outbox_event WHERE id = :id")
				.param("id", id).query(Timestamp::class.java).single()
		)
		assertNull(
			jdbc.sql("SELECT locked_by FROM outbox_event WHERE id = :id")
				.param("id", id).query(String::class.java).optional().orElse(null)
		)
	}

	@Test
	fun `markFailed below the threshold returns to pending with backoff`() {
		val id = freshPending(attemptCount = 0)
		repository.claimBatch(10)
		val before = Instant.now()

		repository.markFailed(listOf(id), "broker down")

		assertEquals("PENDING", statusOf(id))
		assertEquals(1, attemptCount(id))
		// 첫 실패 후 대기는 base(10s). 여유를 두고 8~12초 사이인지만 본다.
		val delay = Duration.between(before, nextAttemptAt(id))
		assertTrue(delay.seconds in 8..12, "실제 대기: ${delay.seconds}s")
		assertEquals(
			"broker down",
			jdbc.sql("SELECT last_error_message FROM outbox_event WHERE id = :id")
				.param("id", id).query(String::class.java).single()
		)
	}

	@Test
	fun `markFailed at the threshold goes dead with the recovery delay`() {
		// maxAttempts=5 이므로 증가 전 4 에서 실패하면 5 가 되어 DEAD 다.
		val id = freshPending(attemptCount = 4)
		repository.claimBatch(10)
		val before = Instant.now()

		repository.markFailed(listOf(id), "serialization failed")

		assertEquals("DEAD", statusOf(id))
		assertEquals(5, attemptCount(id))
		// DEAD 전환 시 next_attempt_at = now() + deadRecoveryDelay(10m). 이게 복구 스캐너의 기준이다.
		val delay = Duration.between(before, nextAttemptAt(id))
		assertTrue(delay.toMinutes() in 9..11, "실제 대기: ${delay.toMinutes()}m")
	}

	@Test
	fun `markFailed clears the lock so zombie recovery does not pick it up`() {
		val id = freshPending()
		repository.claimBatch(10)

		repository.markFailed(listOf(id), "timeout")

		assertNull(
			jdbc.sql("SELECT locked_at FROM outbox_event WHERE id = :id")
				.param("id", id).query(Timestamp::class.java).optional().orElse(null)
		)
	}

	@Test
	fun `markFailed handles a batch sharing one message`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		val ids = (1..3).map { insertOutbox(documentId, versionId) }
		repository.claimBatch(10)

		val updated = repository.markFailed(ids, "broker down")

		assertEquals(3, updated)
		ids.forEach { assertEquals("PENDING", statusOf(it)) }
	}
}
