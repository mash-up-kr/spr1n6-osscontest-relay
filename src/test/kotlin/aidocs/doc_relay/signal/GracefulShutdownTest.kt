package aidocs.doc_relay.signal

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 이 클래스는 stop() 을 직접 호출해 드레인 스레드를 영구히 멈추므로 다른 테스트 클래스와
 * 컨텍스트를 공유하면 안 된다 — PgNotificationListenerTest 와 같은 이유로 클래스 종료 시
 * 컨텍스트를 닫는다.
 */
@TestPropertySource(properties = ["relay.shutdown.drain-timeout=5s"])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class GracefulShutdownTest : RelayIntegrationTest() {

	@Autowired private lateinit var trigger: DrainTrigger

	@Test
	fun `stop drains what was in flight, rejects new signals, and reports not running`() {
		val documentId = seedParents()
		(1L..30L).forEach { insertVersion(documentId, versionNo = it) }
		trigger.signal()

		// signal() 과 stop() 사이에는 진짜 경합이 있다 — DrainTrigger.loop() 는 pending.poll() 이
		// 뭔가를 돌려준 "뒤"에야 draining=true 를 세팅한다. 그 사이에 stop() 이 끼어들면
		// draining==false 인 채로 즉시 리턴해 워커가 시작도 하기 전에 인터럽트당한다 — 그러면
		// 아래 행이 전부 PENDING 으로 남고, "PUBLISHING 이 0행" 이라는 예전 단언은 드레인이
		// 실제로 실행됐든 아니든 항상 참이 되어 버린다. 사이클이 실제로 시작한 걸 관찰할 때까지
		// 짧게 폴링해서 이 경합을 없앤다.
		val cycleStartedDeadline = System.currentTimeMillis() + 2_000
		while (System.currentTimeMillis() < cycleStartedDeadline) {
			val started = jdbc.sql(
				"SELECT count(*) FROM outbox_event WHERE status IN ('PUBLISHING', 'PUBLISHED')"
			).query(Int::class.java).single()
			if (started > 0) break
			Thread.sleep(20)
		}

		trigger.stop()

		assertEquals(
			30,
			jdbc.sql("SELECT count(*) FROM outbox_event WHERE status = 'PUBLISHED'")
				.query(Int::class.java).single(),
			"정상 종료는 진행 중이던 백로그를 끝까지 드레인해야 한다 — 중간에 버리면 안 된다",
		)
		assertEquals(
			0,
			jdbc.sql("SELECT count(*) FROM outbox_event WHERE status = 'PUBLISHING'")
				.query(Int::class.java).single(),
			"정상 종료 뒤 PUBLISHING 으로 남은 행이 없어야 한다",
		)
		assertFalse(trigger.isRunning, "stop() 이후에는 살아있지 않아야 한다")

		val documentId2 = seedParents()
		insertVersion(documentId2)
		trigger.signal()   // 종료 이후의 신호는 무시돼야 한다
		Thread.sleep(500)
		assertEquals(
			"PENDING",
			jdbc.sql("SELECT status FROM outbox_event WHERE document_id = :d")
				.param("d", documentId2).query(String::class.java).single(),
			"종료 중에는 새 신호를 받지 않아야 한다",
		)
	}
}
