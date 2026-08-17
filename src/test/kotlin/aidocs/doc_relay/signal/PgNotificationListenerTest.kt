package aidocs.doc_relay.signal

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// 폴링을 사실상 꺼서 NOTIFY 경로만 남긴다. 폴링이 살아 있으면 이 테스트는 아무것도 증명하지 못한다.
// 베이스 클래스가 relay.listener.enabled=false 로 리스너를 꺼두므로, 이 클래스에서는 다시 켜야 한다
// (Spring @TestPropertySource 는 같은 키에 대해 서브클래스 값이 상속값을 이긴다).
//
// relay.listener.enabled=true 로 PgNotificationListener/DrainTrigger(둘 다 SmartLifecycle) 가
// 도는 컨텍스트다. Spring 의 컨텍스트 캐싱은 빈 그래프만 재사용을 막을 뿐 이 컨텍스트가 언제
// 닫히는지는 보장하지 않는다 — 캐시에 남아 있는 한 이 리스너는 JVM 종료까지 공유 DB 를 계속
// LISTEN 하며, 이후 실행되는 다른 테스트 클래스의 insertVersion() 이 쏘는 진짜 pg_notify 에도
// 반응해 배경 드레인을 일으킨다. @DirtiesContext(AFTER_CLASS) 로 클래스가 끝나면 컨텍스트를 닫아
// stop() 이 실제로 호출되게 한다 (companion object 의 postgres/kafka 컨테이너는 그대로 공유).
@TestPropertySource(
	properties = [
		"relay.polling.interval=1h",
		"relay.listener.enabled=true",
	]
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PgNotificationListenerTest : RelayIntegrationTest() {

	@Autowired private lateinit var listener: PgNotificationListener

	private fun awaitPublishedCount(expected: Int, timeoutMillis: Long): Boolean {
		val deadline = System.currentTimeMillis() + timeoutMillis
		while (System.currentTimeMillis() < deadline) {
			val published = jdbc.sql("SELECT count(*) FROM outbox_event WHERE status = 'PUBLISHED'")
				.query(Int::class.java).single()
			if (published >= expected) return true
			Thread.sleep(200)
		}
		return false
	}

	@Test
	fun `notify alone drives the drain within a second`() {
		val documentId = seedParents()
		insertVersion(documentId)     // 트리거가 outbox INSERT + pg_notify 를 한다

		assertTrue(awaitPublishedCount(1, 10_000), "NOTIFY 만으로 발행되지 않았다")
	}

	@Test
	fun `reconnects after the listen connection is killed and drains what it missed`() {
		assertTrue(listener.connected, "리스너가 처음부터 연결돼 있어야 한다")
		val before = listener.reconnectCount

		// LISTEN 전용 커넥션만 골라 끊는다.
		jdbc.sql(
			"""
			SELECT pg_terminate_backend(pid)
			  FROM pg_stat_activity
			 WHERE application_name = :appName AND pid <> pg_backend_pid()
			""".trimIndent()
		).param("appName", PgNotificationListener.APPLICATION_NAME).query(Boolean::class.java).list()

		// 끊긴 동안 이벤트가 들어온다. 이 NOTIFY 는 유실된다.
		val documentId = seedParents()
		insertVersion(documentId)

		// 재연결 직후 무조건 도는 드레인 1회가 유실분을 회수해야 한다 (spec §5).
		assertTrue(awaitPublishedCount(1, 30_000), "재연결 후 유실분이 회수되지 않았다")
		assertTrue(listener.reconnectCount > before, "재연결 카운터가 오르지 않았다")
	}

	@Test
	fun `notification payload is never used as a lookup key`() {
		// 알림은 깨우는 신호일 뿐이다. 진실의 원천은 항상 DB 쿼리다 (spec §5).
		// 존재하지 않는 UUID 로 직접 NOTIFY 를 쏴도 릴레이가 죽지 않아야 한다.
		jdbc.sql("SELECT pg_notify('outbox_event', :payload)")
			.param("payload", "not-a-real-event-id").query(String::class.java).list()

		val documentId = seedParents()
		insertVersion(documentId)
		assertTrue(awaitPublishedCount(1, 10_000), "가짜 알림 뒤에도 정상 동작해야 한다")
		assertEquals(true, listener.connected)
	}
}
