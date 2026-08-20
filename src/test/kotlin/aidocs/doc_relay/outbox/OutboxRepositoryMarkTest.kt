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

	@Autowired
	private lateinit var properties: aidocs.doc_relay.RelayProperties

	/** id와, 이번 사이클에서 claimBatch 가 돌려준 locked_at 을 함께 돌려준다. */
	private fun freshlyClaimed(attemptCount: Int = 0): Pair<UUID, Instant> {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		val id = insertOutbox(documentId, versionId, attemptCount = attemptCount)
		val claimed = repository.claimBatch(10)
		return id to claimed.single().lockedAt
	}

	private fun nextAttemptAt(id: UUID): Instant =
		jdbc.sql("SELECT next_attempt_at FROM outbox_event WHERE id = :id")
			.param("id", id).query(Timestamp::class.java).single().toInstant()

	private fun attemptCount(id: UUID): Int =
		jdbc.sql("SELECT publish_attempt_count FROM outbox_event WHERE id = :id")
			.param("id", id).query(Int::class.java).single()

	@Test
	fun `markPublished sets published_at and clears the lock`() {
		val (id, claimedAt) = freshlyClaimed()

		repository.markPublished(listOf(id), properties.instanceId, claimedAt)

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
		val (id, claimedAt) = freshlyClaimed(attemptCount = 0)
		val before = Instant.now()

		repository.markFailed(listOf(id), "broker down", properties.instanceId, claimedAt)

		assertEquals("PENDING", statusOf(id))
		assertEquals(1, attemptCount(id))
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
		val (id, claimedAt) = freshlyClaimed(attemptCount = 4)
		val before = Instant.now()

		repository.markFailed(listOf(id), "serialization failed", properties.instanceId, claimedAt)

		assertEquals("DEAD", statusOf(id))
		assertEquals(5, attemptCount(id))
		val delay = Duration.between(before, nextAttemptAt(id))
		assertTrue(delay.toMinutes() in 9..11, "실제 대기: ${delay.toMinutes()}m")
	}

	@Test
	fun `markFailed clears the lock so zombie recovery does not pick it up`() {
		val (id, claimedAt) = freshlyClaimed()

		repository.markFailed(listOf(id), "timeout", properties.instanceId, claimedAt)

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
		val claimed = repository.claimBatch(10)
		val claimedAt = claimed.first().lockedAt

		val updated = repository.markFailed(ids, "broker down", properties.instanceId, claimedAt)

		assertEquals(3, updated)
		ids.forEach { assertEquals("PENDING", statusOf(it)) }
	}

	@Test
	fun `a late failure write cannot undo a publish that already succeeded`() {
		// A 가 행을 선점했다가 좀비 회수로 락을 잃고, B(같은 인스턴스의 다음 사이클)가 다시
		// 잡아 발행에 성공한 뒤, A 가 뒤늦게 예전 claimedAt 으로 실패를 기록하려 하는
		// 시나리오다.
		val (id, staleClaimedAt) = freshlyClaimed()

		jdbc.sql("UPDATE outbox_event SET locked_at = now() - INTERVAL '1 hour' WHERE id = :id")
			.param("id", id).update()
		val reclaimed = repository.reclaimZombies()
		assertEquals(1, reclaimed)
		jdbc.sql("UPDATE outbox_event SET next_attempt_at = now() WHERE id = :id").param("id", id).update()

		val secondClaim = repository.claimBatch(10)
		repository.markPublished(listOf(id), properties.instanceId, secondClaim.single().lockedAt)
		assertEquals("PUBLISHED", statusOf(id))

		val updated = repository.markFailed(listOf(id), "late failure", properties.instanceId, staleClaimedAt)

		assertEquals(0, updated, "예전 사이클의 결과 쓰기는 튕겨야 한다")
		assertEquals("PUBLISHED", statusOf(id), "이미 끝난 발행이 되돌아가면 안 된다")
	}

	@Test
	fun `a different instance's result write is rejected`() {
		val (id, claimedAt) = freshlyClaimed()

		val updated = repository.markPublished(listOf(id), "some-other-instance", claimedAt)

		assertEquals(0, updated)
		assertEquals("PUBLISHING", statusOf(id))
	}
}
