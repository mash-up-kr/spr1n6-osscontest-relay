package aidocs.doc_relay.signal

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// 이 클래스는 아무 프로퍼티도 오버라이드하지 않아 RelayIntegrationTest 의 "기본값 그대로" 컨텍스트를
// DrainerHealthIndicatorTest 등 trigger.stop() 을 직접 부르는 다른 "기본값 그대로" 클래스들과
// 공유해 왔다. 그 클래스들은 @DirtiesContext(AFTER_CLASS) 로 자기 컨텍스트를 닫지만, 전체
// 스위트 규모에서(약 30개 클래스, 서로 다른 프로퍼티 조합마다 컨텍스트 하나씩) 특정 클래스
// 조합이 함께 돌 때 이 클래스가 정지된 DrainTrigger(shuttingDown=true, stop() 이후 signal() 이
// 조용히 무시됨)가 남은 컨텍스트를 물려받는 사례를 실제로 재현해 확인했다 — trigger.signal() 이
// 아무 일도 안 하는데 awaitIdle() 은 "할 일 없음"으로 즉시 true 를 돌려줘 행이 PENDING 에
// 머무른 채 테스트가 실패했다. 아래 프로퍼티 오버라이드는 값 자체는 상위 클래스 기본값과
// 동일하지만, 그 존재만으로 스프링의 컨텍스트 캐시 키가 달라져 이 클래스가 "기본값 그대로"
// 그룹과 컨텍스트를 공유하지 않게 만든다 — 다른 클래스가 무엇을 정지시키든 이 클래스와는
// 무관해진다.
@TestPropertySource(properties = ["relay.polling.interval=1h"])
class DrainTriggerTest : RelayIntegrationTest() {

	@Autowired private lateinit var trigger: DrainTrigger

	@Test
	fun `a signal drains the pending row`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		val id = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId).query(UUID::class.java).single()

		trigger.signal()

		assertTrue(trigger.awaitIdle(20_000), "드레인이 끝나지 않았다")
		assertEquals("PUBLISHED", statusOf(id))
	}

	@Test
	fun `a thousand signals collapse but nothing is left behind`() {
		val documentId = seedParents()
		(1L..5L).forEach { insertVersion(documentId, versionNo = it) }

		repeat(1000) { trigger.signal() }

		assertTrue(trigger.awaitIdle(30_000), "드레인이 끝나지 않았다")
		assertEquals(
			0,
			jdbc.sql("SELECT count(*) FROM outbox_event WHERE status <> 'PUBLISHED'")
				.query(Int::class.java).single(),
		)
	}

	@Test
	fun `signal returns immediately`() {
		val started = System.currentTimeMillis()
		trigger.signal()
		assertTrue(System.currentTimeMillis() - started < 500, "signal 이 블로킹됐다")
	}
}
