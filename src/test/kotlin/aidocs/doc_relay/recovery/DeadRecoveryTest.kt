package aidocs.doc_relay.recovery

import aidocs.doc_relay.outbox.OutboxRepository
import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.test.assertEquals

@TestPropertySource(
	properties = [
		"relay.polling.interval=1h",
		"relay.zombie.scan-interval=1h",
		"relay.dead.recovery-scan-interval=1h",
	]
)
class DeadRecoveryTest : RelayIntegrationTest() {

	@Autowired private lateinit var scheduler: DeadRecoveryScheduler
	@Autowired private lateinit var repository: OutboxRepository

	private fun deadRow(nextAttemptAt: Instant): UUID {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		return insertOutbox(
			documentId, versionId,
			status = "DEAD", attemptCount = 5, nextAttemptAt = nextAttemptAt,
		)
	}

	private fun attemptCount(id: UUID): Int =
		jdbc.sql("SELECT publish_attempt_count FROM outbox_event WHERE id = :id")
			.param("id", id).query(Int::class.java).single()

	@Test
	fun `recovers a dead row whose recovery delay has passed`() {
		val id = deadRow(Instant.now().minus(1, ChronoUnit.MINUTES))

		assertEquals(1, repository.recoverDead())
		assertEquals("PENDING", statusOf(id))
	}

	@Test
	fun `resets the attempt counter on recovery`() {
		val id = deadRow(Instant.now().minus(1, ChronoUnit.MINUTES))

		scheduler.recover()

		assertEquals(0, attemptCount(id))
	}

	@Test
	fun `leaves a freshly dead row alone until the delay elapses`() {
		val id = deadRow(Instant.now().plus(9, ChronoUnit.MINUTES))

		assertEquals(0, scheduler.recover())
		assertEquals("DEAD", statusOf(id))
	}

	@Test
	fun `hold removes a row from recovery forever`() {
		val id = deadRow(Instant.now().minus(1, ChronoUnit.MINUTES))

		repository.hold(id)

		repeat(3) { assertEquals(0, scheduler.recover()) }
		assertEquals("DEAD", statusOf(id))
	}

	@Test
	fun `release puts a held row back into recovery`() {
		val id = deadRow(Instant.now().minus(1, ChronoUnit.MINUTES))
		repository.hold(id)

		repository.release(id)

		assertEquals(1, repository.recoverDead())
		assertEquals("PENDING", statusOf(id))
	}

	@Test
	fun `republish makes a dead row immediately due`() {
		val id = deadRow(Instant.now().plus(1, ChronoUnit.HOURS))

		repository.republish(id)

		assertEquals("PENDING", statusOf(id))
		assertEquals(0, attemptCount(id))
	}
}
