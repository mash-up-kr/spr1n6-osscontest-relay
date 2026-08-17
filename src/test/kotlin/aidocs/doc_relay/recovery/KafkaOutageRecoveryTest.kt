package aidocs.doc_relay.recovery

import aidocs.doc_relay.outbox.OutboxDrainer
import aidocs.doc_relay.outbox.OutboxRepository
import aidocs.doc_relay.support.RelayIntegrationTest
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import java.util.UUID
import kotlin.test.assertEquals

/**
 * spec §10 시나리오 4. 백오프를 0 으로 줄여 재시도 소진을 빠르게 관찰한다.
 *
 * 카프카 다운은 브로커/커넥션 오버라이드로 흉내내지 않는다. @ServiceConnection 이
 * Testcontainers 브로커의 KafkaConnectionDetails 빈을 등록하고 KafkaProducerConfig 가
 * bootstrap.servers 를 거기서 직접 읽으므로 spring.kafka.bootstrap-servers 를
 * @TestPropertySource 로 덮어써도 조용히 무시된다 (Task 7 에서 이미 확인됨).
 *
 * 대신 max.request.size(기본 1MB) 를 넘는 payload 로 실제 카프카 클라이언트가
 * ensureValidRecordSize() 에서 네트워크 호출 전에 client-side 로 RecordTooLargeException 을
 * 던지게 만든다 (Task 7·8 에서 이미 검증한 기법). 모든 발행 시도가 동일하게 실패하므로
 * 브로커를 조작하지 않고도 재시도 소진 -> DEAD -> 자동 복구 전 과정을 실제 브로커
 * 대상으로 결정론적으로 관찰할 수 있다.
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

	@Autowired private lateinit var drainer: OutboxDrainer
	@Autowired private lateinit var repository: OutboxRepository
	@Autowired private lateinit var registry: MeterRegistry

	@Test
	fun `exhausts retries into dead then recovers automatically`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		val id = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId).query(UUID::class.java).single()
		// max.request.size 기본값(1MB)을 넘겨 실제 카프카 클라이언트가 매번 거부하게 만든다.
		// 브로커를 죽이는 게 아니라 client-side 검증이라 결정론적이고 타이밍 의존이 없다.
		jdbc.sql(
			"UPDATE outbox_event SET payload = jsonb_build_object('pad', repeat('a', 1200000)) WHERE id = :id"
		).param("id", id).update()

		drainer.drainOnce()                       // 1회 실패 -> PENDING, count=1 (maxAttempts=2 미도달)
		assertEquals("PENDING", statusOf(id))

		drainer.drainOnce()                       // 2회 실패 -> maxAttempts(2) 도달 -> DEAD
		assertEquals("DEAD", statusOf(id))
		// relay.dead.transition.total 은 spec 이 "순환 관측의 유일한 창"이라 부르는 카운터다
		// (recoverDead() 가 publish_attempt_count 를 리셋해 DB 로는 순환 여부를 못 본다).
		// 이 테스트에는 행이 하나뿐이고 DEAD 전환도 방금 그 한 번뿐이므로 정확히 1이어야 한다.
		// 이 클래스의 프로퍼티 조합은 다른 테스트 클래스와 겹치지 않아 컨텍스트가 이 테스트
		// 전용이므로, 카운터에 다른 테스트의 값이 섞여 들어올 수 없다.
		assertEquals(1.0, registry.counter("relay.dead.transition.total").count())

		assertEquals(1, repository.recoverDead())  // recovery-delay=0s 이므로 즉시 복구된다
		assertEquals("PENDING", statusOf(id))
		assertEquals(
			0,
			jdbc.sql("SELECT publish_attempt_count FROM outbox_event WHERE id = :id")
				.param("id", id).query(Int::class.java).single(),
		)
	}
}
