package aidocs.doc_relay.admin

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.endpoint.annotation.Endpoint
import org.springframework.core.env.Environment
import org.springframework.test.context.TestPropertySource
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@TestPropertySource(
	properties = [
		"relay.polling.interval=1h",
		"relay.zombie.scan-interval=1h",
		"relay.dead.recovery-scan-interval=1h",
	]
)
class OutboxEndpointTest : RelayIntegrationTest() {

	@Autowired private lateinit var endpoint: OutboxEndpoint
	@Autowired private lateinit var environment: Environment

	private fun deadRow(): java.util.UUID {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		return insertOutbox(
			documentId, versionId, status = "DEAD", attemptCount = 5,
			nextAttemptAt = Instant.now().plus(10, ChronoUnit.MINUTES),
		)
	}

	@Test
	fun `is an actuator endpoint not a controller`() {
		val annotation = OutboxEndpoint::class.java.getAnnotation(Endpoint::class.java)
		assertEquals("outbox", annotation.id)
	}

	@Test
	fun `is exposed over the web`() {
		val include = environment.getProperty("management.endpoints.web.exposure.include") ?: ""
		assertTrue(include.contains("outbox"), "노출 목록에 outbox 가 없다: $include")
	}

	@Test
	fun `summary reports counts by status`() {
		deadRow()

		val summary = endpoint.summary()

		assertEquals(1, summary.dead)
		assertEquals(0, summary.held)
	}

	@Test
	fun `dead listing marks held rows`() {
		val id = deadRow()

		assertEquals(false, endpoint.byStatus("dead").single().held)

		endpoint.act(id.toString(), "HOLD")

		val view = endpoint.byStatus("dead").single()
		assertEquals(true, view.held)
		assertEquals(5, view.publishAttemptCount)
		assertEquals(id.toString(), view.eventId)
	}

	@Test
	fun `republish makes the row immediately due`() {
		val id = deadRow()

		endpoint.act(id.toString(), "REPUBLISH")

		// act() 는 republish() 직후 trigger.signal() 도 호출한다. 이 클래스는 polling/listener 가
		// 꺼져 있어도 DrainTrigger 자체는 살아 있으므로, statusOf(id) 로 "PENDING" 을 확인하면
		// 실제 드레인 스레드와 경합해 PUBLISHING/PUBLISHED 로 이미 넘어가 버릴 수 있다
		// (DrainTriggerTest 가 signal 후 awaitIdle 을 쓰는 이유와 같다). republish 가 남기는
		// 불변 사실 — 시도 횟수 리셋과 즉시 만기 — 을 행에서 직접 확인해 드레인 진행 여부와
		// 무관하게 "즉시 재발행 대상이 됐다" 를 검증한다.
		val row = jdbc.sql(
			"SELECT publish_attempt_count, next_attempt_at <= now() AS due FROM outbox_event WHERE id = :id"
		).param("id", id).query { rs, _ -> rs.getInt("publish_attempt_count") to rs.getBoolean("due") }.single()
		assertEquals(0, row.first, "republish 는 시도 횟수를 리셋해야 한다")
		assertTrue(row.second, "republish 직후에는 즉시 처리 대상이어야 한다")
	}

	@Test
	fun `release undoes a hold`() {
		val id = deadRow()
		endpoint.act(id.toString(), "HOLD")

		endpoint.act(id.toString(), "RELEASE")

		assertEquals(false, endpoint.byStatus("dead").single().held)
	}

	@Test
	fun `rejects an unknown action`() {
		val id = deadRow()
		assertFailsWith<IllegalArgumentException> { endpoint.act(id.toString(), "DELETE") }
	}

	@Test
	fun `force_republish makes a published row due again`() {
		// PUBLISHED 행을 대상으로 하는 파괴적 동작이라 일반 REPUBLISH 와는 별도 액션으로 뒀다.
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		val id = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId).query(java.util.UUID::class.java).single()
		jdbc.sql("UPDATE outbox_event SET status = 'PUBLISHED', published_at = now() WHERE id = :id")
			.param("id", id).update()

		endpoint.act(id.toString(), "FORCE_REPUBLISH")

		val row = jdbc.sql(
			"SELECT status, publish_attempt_count, next_attempt_at <= now() AS due FROM outbox_event WHERE id = :id"
		).param("id", id).query { rs, _ -> Triple(rs.getString("status"), rs.getInt("publish_attempt_count"), rs.getBoolean("due")) }.single()
		assertEquals("PENDING", row.first)
		assertEquals(0, row.second)
		assertTrue(row.third)
	}

	@Test
	fun `force_republish does nothing to a row that is not published`() {
		val id = deadRow()

		endpoint.act(id.toString(), "FORCE_REPUBLISH")

		assertEquals("DEAD", statusOf(id))
	}

	@Test
	fun `rejects an unknown status selector`() {
		assertFailsWith<IllegalArgumentException> { endpoint.byStatus("banana") }
	}
}
