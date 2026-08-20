package aidocs.doc_relay

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.health.contributor.Status
import org.springframework.test.annotation.DirtiesContext
import kotlin.test.assertEquals

/**
 * 드레인 스레드가 완전히 죽으면(재시작해야 낫는 문제) liveness 가 DOWN 이어야 재시작 정책이
 * 파드를 되살린다. 반대로 LISTEN(Postgres 알림 채널) 커넥션만 끊긴 상태는 폴링이 여전히
 * 동작을 보장하므로 DOWN 으로 잡으면 멀쩡한 파드가 불필요하게 재시작된다.
 * stop() 은 드레인 스레드를 되돌릴 수 없이 멈추므로 클래스가 끝나면 컨텍스트를 버린다.
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DrainerHealthIndicatorTest : RelayIntegrationTest() {

	@Autowired private lateinit var indicator: DrainerHealthIndicator
	@Autowired private lateinit var trigger: aidocs.doc_relay.signal.DrainTrigger

	@Test
	fun `stays up while the listener is disconnected, then goes down once the drain thread stops`() {
		// 두 단언을 한 메서드에 담는다 — stop() 이 컨텍스트를 되돌릴 수 없는 상태로 만들어
		// 버리므로, 별도 메서드로 나누면 JUnit5 의 기본 실행 순서가 어느 쪽이 먼저인지
		// 보장하지 않아 "down" 이 "up" 보다 먼저 실행될 위험이 있다.
		//
		// RelayIntegrationTest 기본값은 relay.listener.enabled=false 라 리스너가 애초에
		// 연결되지 않는다. 그래도 폴링이 동작을 계속 보장하므로 UP 이어야 하고, 연결 여부는
		// details 에만 신호로 남긴다.
		val upHealth = indicator.health()
		assertEquals(Status.UP, upHealth.status)
		assertEquals(false, upHealth.details["listenerConnected"])

		// 정상 종료(stop())도 "드레인 스레드가 죽은" 경우로 간주해 DOWN 이어야 한다.
		trigger.stop()

		assertEquals(Status.DOWN, indicator.health().status)
	}
}
