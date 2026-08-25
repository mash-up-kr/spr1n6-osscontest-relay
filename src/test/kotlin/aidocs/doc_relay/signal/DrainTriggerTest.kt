package aidocs.doc_relay.signal

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// 아래 프로퍼티 오버라이드는 값이 상위 클래스 기본값과 같다. 값을 바꾸려는 것이 아니라
// 스프링의 컨텍스트 캐시 키를 다르게 만드는 것이 목적이다.
//
// 오버라이드가 없으면 trigger.stop() 을 직접 부르는 다른 "기본값 그대로" 클래스와 컨텍스트를
// 공유하게 되고, 정지된 DrainTrigger(shuttingDown=true)를 물려받으면 signal() 이 조용히 무시된다.
// 그때 awaitIdle() 은 "할 일 없음"으로 즉시 true 를 돌려주고 행은 PENDING 에 남는다.
@TestPropertySource(properties = ["relay.polling.interval=1h"])
class DrainTriggerTest : RelayIntegrationTest() {

	@Autowired private lateinit var trigger: DrainTrigger

	@Test
	fun `a signal drains the pending row`() {
		val documentId = seedParents()
		val documentVersionId = insertVersion(documentId)
		val id = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", documentVersionId).query(UUID::class.java).single()

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
