package aidocs.doc_relay.outbox

import aidocs.doc_relay.recovery.ZombieRecoveryScheduler
import aidocs.doc_relay.signal.DrainTrigger
import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestPropertySource(properties = ["relay.polling.interval=1h", "relay.zombie.scan-interval=1h"])
class DuplicateDeliveryContractTest : RelayIntegrationTest() {

	@Autowired private lateinit var repository: OutboxRepository
	@Autowired private lateinit var publisher: KafkaPublisher
	@Autowired private lateinit var zombieScheduler: ZombieRecoveryScheduler
	@Autowired private lateinit var drainTrigger: DrainTrigger

	@Test
	fun `a crash between publish and mark yields a duplicate, which is contractual`() {
		// spec §4: Kafka ack 를 받고 markPublished 를 커밋하기 전에 죽으면 중복 발행된다.
		// at-least-once 계약상 정상이며 워커 멱등성(source_event_id UNIQUE, 청크 UPSERT)이
		// 흡수한다. 이걸 없애려면 Kafka 트랜잭션과 DB 트랜잭션을 묶어야 하는데 그럴 가치가 없다.
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		val id = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId).query(UUID::class.java).single()

		// 발행까지만 하고 결과 반영을 건너뛴다 = 반영 직전 크래시
		val claimed = repository.claimBatch(10)
		publisher.publish(claimed)
		assertEquals("PUBLISHING", statusOf(id), "반영 전이므로 PUBLISHING 으로 남아야 한다")

		// 좀비 회수가 집어 다시 발행한다 -> 같은 eventId 가 카프카에 두 번 들어간다
		jdbc.sql("UPDATE outbox_event SET locked_at = now() - INTERVAL '1 hour' WHERE id = :id")
			.param("id", id).update()
		assertEquals(1, zombieScheduler.reclaim())
		// reclaim()은 spec §6①대로 백오프를 적용해 next_attempt_at을 미래로 미룬다(기본 base=10s).
		// 실제로는 그 시간이 지난 뒤 다음 드레인 사이클이 다시 집어 가지만, 테스트에서 실제로
		// 10초를 기다리진 않는다 — next_attempt_at을 앞당긴 것으로 시간 경과를 흉내낸다.
		jdbc.sql("UPDATE outbox_event SET next_attempt_at = now() WHERE id = :id")
			.param("id", id).update()

		// reclaim() 이 이미 trigger.signal() 을 쏴서 백그라운드 드레인이 도는 중이다. 여기서
		// 별도로 drainer.drainOnce() 를 또 부르면 foreground 와 background 가 같은 행의
		// claim 을 놓고 경합한다 — background 가 claim 만 이기고 발행을 아직 못 끝낸 채로
		// foreground 의 "0건 찾음" 반환이 먼저 끝나 다음 줄의 단언이 중간 상태(PUBLISHING)를
		// 보고 실패할 수 있다. 행위자를 하나로 줄이고 그게 끝나기를 기다린다.
		assertTrue(drainTrigger.awaitIdle(10_000), "재발행 드레인이 끝나지 않았다")

		assertEquals("PUBLISHED", statusOf(id))
		// 카프카에는 같은 eventId 가 2건 있다. 이것이 계약이며 결함이 아니다.
	}
}
