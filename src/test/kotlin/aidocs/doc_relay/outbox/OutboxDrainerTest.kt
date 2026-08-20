package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import aidocs.doc_relay.support.RelayIntegrationTest
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class OutboxDrainerTest : RelayIntegrationTest() {

	@Autowired private lateinit var drainer: OutboxDrainer
	@Autowired private lateinit var repository: OutboxRepository
	@Autowired private lateinit var properties: RelayProperties
	@Autowired private lateinit var registry: MeterRegistry

	@Test
	fun `drains a pending row all the way to published`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)     // 트리거가 outbox 행을 만든다
		val id = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId).query(java.util.UUID::class.java).single()

		val drained = drainer.drainOnce()

		assertEquals(1, drained)
		assertEquals("PUBLISHED", statusOf(id))
	}

	@Test
	fun `drains nothing when there is nothing due`() {
		assertEquals(0, drainer.drainOnce())
	}

	@Test
	fun `drainUntilEmpty keeps cycling past one batch`() {
		val documentId = seedParents()
		// batchSize 기본값은 100. 트리거로 120건을 만들어 두 사이클이 필요하게 한다.
		(1L..120L).forEach { insertVersion(documentId, versionNo = it) }

		val total = drainer.drainUntilEmpty()

		assertEquals(120, total)
		assertEquals(
			0,
			jdbc.sql("SELECT count(*) FROM outbox_event WHERE status <> 'PUBLISHED'")
				.query(Int::class.java).single(),
		)
	}

	@Test
	fun `drainOnce routes each row's own error message, never another row's`() {
		// payload 는 실제 jsonb 컬럼이라 Postgres 가 저장 시점에 문법을 검증한다 — 깨진 JSON
		// 문자열은 UPDATE 자체가 SQL 에러로 거부되어 EnvelopeAssembler 까지 도달할 수 없다
		// (직접 확인함). 대신 두 행의 payload 크기를 서로 다르게 키워 Kafka 의
		// max.request.size(기본 1MB) 를 서로 다른 크기로 넘기게 만든다. KafkaProducer.doSend()
		// 는 ensureValidRecordSize() 에서 실제 직렬화 바이트 수를 메시지에 그대로 박아 동기적으로
		// RecordTooLargeException 을 던지므로("The message is <N> bytes when serialized..."),
		// 두 행은 서로 다른 진짜 예외 메시지를 얻는다 — 브로커 상태나 타이밍에 기대지 않는,
		// 내용(크기)에 의해 결정되는 값이다.
		val documentId = seedParents()
		val versionId1 = insertVersion(documentId, versionNo = 1)
		val versionId2 = insertVersion(documentId, versionNo = 2)
		val id1 = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId1).query(UUID::class.java).single()
		val id2 = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId2).query(UUID::class.java).single()
		growPayload(id1, 1_200_000)
		growPayload(id2, 2_000_000)

		drainer.drainOnce()

		val error1 = jdbc.sql("SELECT last_error_message FROM outbox_event WHERE id = :id")
			.param("id", id1).query(String::class.java).single()
		val error2 = jdbc.sql("SELECT last_error_message FROM outbox_event WHERE id = :id")
			.param("id", id2).query(String::class.java).single()

		// 뒤섞이거나 하나로 합쳐졌다면 (예: markFailed 를 failed 값 전체를 flatten 하고
		// 메시지 하나만 골라 한 번에 호출) 여기서 두 값이 같아진다.
		// FailureClassifier 도입 이후 RecordTooLargeException 은 영구 실패로 분류돼 DEAD 로
		// 간다. 이 테스트가 원래 증명하려는 것 — 배치 안에서 각 행이 자기 자신의 에러 메시지만
		// 받고 다른 행의 메시지와 섞이지 않는다 — 는 상태가 DEAD 로 바뀌어도 그대로 유효하다.
		assertNotEquals(error1, error2)
		assertEquals("DEAD", statusOf(id1))
		assertEquals("DEAD", statusOf(id2))
	}

	@Test
	fun `markPublished rejects a mismatched claimedAt and records it as a stale write`() {
		// drainer 의 internal 헬퍼를 직접 호출해 reportStaleness -> metrics.recordStaleWrite
		// 배선이 drainOnce() 전체를 거치지 않고도 실제로 도는지 확인한다. SQL 레벨의 소유권
		// 거부 자체는 OutboxRepositoryMarkTest 에서 이미 검증했다.
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		val id = insertOutbox(documentId, versionId)
		val claimed = repository.claimBatch(10)
		val byId = claimed.associateBy { it.id }
		val before = registry.counter("relay.stale.write.total").count()

		drainer.markPublishedSafely(listOf(id), properties.instanceId, Instant.now().minusSeconds(999), byId)

		assertEquals(before + 1, registry.counter("relay.stale.write.total").count())
		assertEquals("PUBLISHING", statusOf(id), "소유권이 안 맞는 쓰기는 행을 건드리면 안 된다")
	}

	@Test
	fun `markFailed rejects a mismatched claimedAt and records it as a stale write`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		val id = insertOutbox(documentId, versionId)
		repository.claimBatch(10)
		val before = registry.counter("relay.stale.write.total").count()

		drainer.markFailedSafely(listOf(id), "stale write test", properties.instanceId, Instant.now().minusSeconds(999))

		assertEquals(before + 1, registry.counter("relay.stale.write.total").count())
		assertEquals("PUBLISHING", statusOf(id), "소유권이 안 맞는 쓰기는 행을 건드리면 안 된다")
	}

	/** payload 를 [padLength] 길이의 문자열을 품은 (여전히 유효한) 큰 JSON 으로 바꾼다. */
	private fun growPayload(id: UUID, padLength: Int) {
		jdbc.sql("UPDATE outbox_event SET payload = jsonb_build_object('pad', repeat('a', :len)) WHERE id = :id")
			.param("len", padLength)
			.param("id", id)
			.update()
	}
}
