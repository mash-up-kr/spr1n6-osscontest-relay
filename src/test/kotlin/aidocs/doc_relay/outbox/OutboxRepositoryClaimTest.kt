package aidocs.doc_relay.outbox

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OutboxRepositoryClaimTest : RelayIntegrationTest() {

	@Autowired
	private lateinit var repository: OutboxRepository

	@Test
	fun `claims pending rows that are due and marks them publishing`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()   // 트리거가 만든 행 제거, 상태를 직접 통제한다
		val id = insertOutbox(documentId, versionId)

		val claimed = repository.claimBatch(10)

		assertEquals(1, claimed.size)
		assertEquals(id, claimed.single().id)
		assertEquals("INDEXING_REQUESTED", claimed.single().eventType)
		assertEquals("PUBLISHING", statusOf(id))
	}

	@Test
	fun `reads a null document_version_id as null instead of zero`() {
		// DOCUMENT_DELETED 는 문서 단위 이벤트라 document_version_id 가 NULL 이다.
		// JDBC 의 getLong 은 SQL NULL 에 예외를 내지 않고 0 을 돌려주므로, wasNull 확인을 빼면
		// 없는 값이 0 번 버전으로 둔갑해 오류 없이 그대로 발행된다. 실패해도 조용하다는 게 요점이다.
		val documentId = seedParents()
		insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		insertOutbox(documentId, documentVersionId = null, eventType = "DOCUMENT_DELETED")

		val claimed = repository.claimBatch(10).single()

		assertEquals("DOCUMENT_DELETED", claimed.eventType)
		assertNull(claimed.documentVersionId)
	}

	@Test
	fun `does not claim rows whose next_attempt_at is in the future`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		insertOutbox(documentId, versionId, nextAttemptAt = Instant.now().plus(1, ChronoUnit.HOURS))

		assertTrue(repository.claimBatch(10).isEmpty())
	}

	@Test
	fun `does not claim rows that are not pending`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		insertOutbox(documentId, versionId, status = "PUBLISHED")
		insertOutbox(documentId, versionId, status = "DEAD")

		assertTrue(repository.claimBatch(10).isEmpty())
	}

	@Test
	fun `respects the limit and claims oldest first`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		val base = Instant.now().minus(1, ChronoUnit.HOURS)
		val first = insertOutbox(documentId, versionId, nextAttemptAt = base)
		insertOutbox(documentId, versionId, nextAttemptAt = base.plusSeconds(60))
		insertOutbox(documentId, versionId, nextAttemptAt = base.plusSeconds(120))

		val claimed = repository.claimBatch(2)

		assertEquals(2, claimed.size)
		assertEquals(first, claimed.first().id)
	}

	@Test
	fun `records the instance id on the locked row`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		val id = insertOutbox(documentId, versionId)

		repository.claimBatch(10)

		val lockedBy = jdbc.sql("SELECT locked_by FROM outbox_event WHERE id = :id")
			.param("id", id).query(String::class.java).single()
		assertTrue(lockedBy.isNotBlank())
		val lockedAt = jdbc.sql("SELECT locked_at FROM outbox_event WHERE id = :id")
			.param("id", id).query(java.sql.Timestamp::class.java).single()
		assertTrue(lockedAt != null)
	}

	@Test
	fun `a claimed batch shares the same locked_at`() {
		// 소유권 확인을 위한 배치 UPDATE 전체가 "한 배치는 같은 locked_at 을 공유한다"는 전제
		// 위에 있다 — 그 전제가 깨지면 나중의 결과 쓰기(mark)가 어느 배치에 속하는지 구분할 수
		// 없다.
		val documentId = seedParents()
		(1L..10L).forEach { insertVersion(documentId, versionNo = it) }

		val claimed = repository.claimBatch(10)

		assertEquals(10, claimed.size)
		assertEquals(
			1, claimed.map { it.lockedAt }.distinct().size,
			"한 배치로 잡은 행은 전부 같은 locked_at 을 가져야 한다",
		)
	}
}
