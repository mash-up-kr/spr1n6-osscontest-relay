package aidocs.doc_relay.recovery

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals

@TestPropertySource(properties = ["relay.polling.interval=1h", "relay.zombie.scan-interval=1h"])
class ZombieRecoveryTest : RelayIntegrationTest() {

	@Autowired private lateinit var scheduler: ZombieRecoveryScheduler

	private fun attemptCount(id: java.util.UUID): Int =
		jdbc.sql("SELECT publish_attempt_count FROM outbox_event WHERE id = :id")
			.param("id", id).query(Int::class.java).single()

	@Test
	fun `reclaims a row stuck in publishing past the lock timeout`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		val id = insertOutbox(
			documentId, versionId,
			status = "PUBLISHING",
			lockedAt = Instant.now().minus(30, ChronoUnit.MINUTES),
		)

		assertEquals(1, scheduler.reclaim())
		assertEquals("PENDING", statusOf(id))
	}

	@Test
	fun `increments the attempt count on reclaim`() {
		// 올리지 않으면 릴레이를 반복해서 죽이는 행이 회수 <-> 재시도를 무한 반복한다 (spec §6-1).
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		val id = insertOutbox(
			documentId, versionId,
			status = "PUBLISHING", attemptCount = 2,
			lockedAt = Instant.now().minus(30, ChronoUnit.MINUTES),
		)

		scheduler.reclaim()

		assertEquals(3, attemptCount(id))
	}

	@Test
	fun `always returns to pending even past the max attempts`() {
		// 회수 시점은 "락이 만료됐다"만 아는 시점이다. DEAD 판정은 실제 발행 실패 때만 한다 (spec §6-1).
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		val id = insertOutbox(
			documentId, versionId,
			status = "PUBLISHING", attemptCount = 9,
			lockedAt = Instant.now().minus(30, ChronoUnit.MINUTES),
		)

		scheduler.reclaim()

		assertEquals("PENDING", statusOf(id))
	}

	@Test
	fun `leaves fresh locks alone`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		val id = insertOutbox(documentId, versionId, status = "PUBLISHING", lockedAt = Instant.now())

		assertEquals(0, scheduler.reclaim())
		assertEquals("PUBLISHING", statusOf(id))
	}
}
