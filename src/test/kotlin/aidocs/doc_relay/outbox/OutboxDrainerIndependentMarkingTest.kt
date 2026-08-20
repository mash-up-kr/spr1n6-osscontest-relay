package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import aidocs.doc_relay.support.RelayIntegrationTest
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals

/**
 * 성공 기록과 실패 기록은 서로 독립적이어야 한다 — 하나가 던져도 다른 하나는 계속 실행되고,
 * 지표는 항상 갱신돼야 한다. Instant.MAX 는 timestamptz(타임존 포함 타임스탬프) 컬럼 범위를
 * 벗어나 바인딩 시 예외를 던지므로, "성공 기록 쓰기 자체가 실패하는 상황"을 브로커나 DB를
 * 실제로 죽이지 않고 결정론적으로 재현할 수 있다.
 */
class OutboxDrainerIndependentMarkingTest : RelayIntegrationTest() {

	@Autowired private lateinit var drainer: OutboxDrainer
	@Autowired private lateinit var repository: OutboxRepository
	@Autowired private lateinit var properties: RelayProperties
	@Autowired private lateinit var registry: MeterRegistry

	@Test
	fun `a broken success write does not block failure write or the failure metric`() {
		val documentId = seedParents()
		val versionId1 = insertVersion(documentId, versionNo = 1)
		val versionId2 = insertVersion(documentId, versionNo = 2)
		val id1 = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId1).query(UUID::class.java).single()
		val id2 = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId2).query(UUID::class.java).single()
		val claimed = repository.claimBatch(10)
		val byId = claimed.associateBy { it.id }
		val claimedAt = claimed.first().lockedAt
		val before = registry.counter("relay.mark.failure.total").count()

		drainer.markPublishedSafely(listOf(id1), properties.instanceId, Instant.MAX, byId)
		drainer.markFailedSafely(listOf(id2), "unrelated failure", properties.instanceId, claimedAt)

		assertEquals(before + 1, registry.counter("relay.mark.failure.total").count(), "성공 반영 실패가 카운터에 잡혀야 한다")
		assertEquals("PENDING", statusOf(id2), "실패 반영은 독립적으로 계속 실행돼야 한다")
		assertEquals("PUBLISHING", statusOf(id1), "성공 반영 실패는 행 상태를 바꾸지 않아야 한다")
	}
}
