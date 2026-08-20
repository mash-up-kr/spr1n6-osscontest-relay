package aidocs.doc_relay.recovery

import aidocs.doc_relay.RelayProperties
import aidocs.doc_relay.outbox.OutboxRepository
import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import java.util.UUID
import kotlin.test.assertEquals

/**
 * 재시도를 소진하면 DEAD 로 넘어가고, 복구 지연이 지나면 자동으로 되살아나는 흐름을
 * 검증한다.
 *
 * FailureClassifier 도입 전에는 max.request.size 초과(RecordTooLargeException)로 실제
 * 카프카 브로커를 대상으로 이 흐름을 결정론적으로 재현했다. 그런데 FailureClassifier 도입
 * 이후 그 예외는 영구 실패로 분류돼 재시도 없이 1회 만에 DEAD 로 가므로(그 경로는
 * FailureClassificationDrainTest 가 따로 검증한다) 더는 "일시 실패가 반복되다 소진"을
 * 흉내내는 수단으로 쓸 수 없다. 이 테스트가 실제로 검증하려는 것은 카프카 장애 자체가 아니라
 * OutboxRepository.markFailed 의 소진→DEAD→복구 계산이므로, 그 계층을 직접 구동해 같은 것을
 * 결정론적으로 증명한다.
 */
@TestPropertySource(
	properties = [
		"relay.polling.interval=1h",
		"relay.zombie.scan-interval=1h",
		"relay.dead.recovery-scan-interval=1h",
		"relay.backoff.base=0s",
		"relay.backoff.max=0s",
		"relay.backoff.max-attempts=2",
		"relay.dead.recovery-delay=0s",
	]
)
class KafkaOutageRecoveryTest : RelayIntegrationTest() {

	@Autowired private lateinit var repository: OutboxRepository
	@Autowired private lateinit var properties: RelayProperties

	@Test
	fun `exhausts retries into dead then recovers automatically`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		val id = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId).query(UUID::class.java).single()

		val firstClaim = repository.claimBatch(10)
		repository.markFailed(listOf(id), "broker down", properties.instanceId, firstClaim.single().lockedAt)
		assertEquals("PENDING", statusOf(id))   // 1회 실패, maxAttempts(2) 미도달

		jdbc.sql("UPDATE outbox_event SET next_attempt_at = now() WHERE id = :id").param("id", id).update()
		val secondClaim = repository.claimBatch(10)
		repository.markFailed(listOf(id), "broker down", properties.instanceId, secondClaim.single().lockedAt)
		assertEquals("DEAD", statusOf(id))   // 2회 실패, maxAttempts(2) 도달

		assertEquals(1, repository.recoverDead())   // recovery-delay=0s 이므로 즉시 복구된다
		assertEquals("PENDING", statusOf(id))
		assertEquals(
			0,
			jdbc.sql("SELECT publish_attempt_count FROM outbox_event WHERE id = :id")
				.param("id", id).query(Int::class.java).single(),
		)
	}
}
