package aidocs.doc_relay.observability

import aidocs.doc_relay.signal.PgNotificationListener
import aidocs.doc_relay.support.RelayIntegrationTest
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertEquals

// relay.listener.enabled=true 를 이 클래스에서만 켠다. RelayMetricsTest 와 같은 클래스에 두면
// @TestPropertySource 가 클래스 단위로 적용돼 다른 테스트들의 insertVersion() 이 진짜 pg_notify 를
// 쏘게 되고, 항상 도는 DrainTrigger 가 그 테스트들의 명시적 drainOnce() 와 경합한다 —
// Task 12/13 에서 이미 두 번 발견하고 고친 것과 같은 종류의 버그다. 그래서 별도 클래스로 분리한다.
// Spring 이 프로퍼티 집합별로 캐싱하는 건 ApplicationContext(빈 그래프)뿐이다 — companion object 의
// postgres/kafka 컨테이너는 Spring 밖의 순수 Kotlin 싱글턴이라 모든 컨텍스트가 그대로 공유한다.
// 이 클래스가 끝나도 컨텍스트가 캐시에 남아 있으면 이 클래스의 PgNotificationListener/DrainTrigger
// (둘 다 SmartLifecycle) 가 JVM 종료까지 백그라운드에서 계속 돌면서, 그 뒤에 실행되는 다른 테스트
// 클래스가 같은 공유 DB 에 쏘는 insertVersion() 의 진짜 pg_notify 에 반응해 경합을 일으킨다 —
// "다른 테스트 클래스 자체"가 아니라 "그 클래스의 이미 끝난 컨텍스트에 남은 배경 스레드"가 원인이라
// 캐싱만으로는 격리되지 않는다. @DirtiesContext(AFTER_CLASS) 로 이 클래스의 컨텍스트를 명시적으로
// 닫아 stop() 이 호출되게 한다. postgres/kafka 필드는 여전히 안 건드리므로 다른 클래스가 쓸
// 컨테이너는 그대로 살아 있다.
@TestPropertySource(
	properties = [
		"relay.polling.interval=1h",
		"relay.zombie.scan-interval=1h",
		"relay.dead.recovery-scan-interval=1h",
		"relay.metrics.gauge-refresh-interval=1h",
		"relay.listener.enabled=true",
	]
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RelayMetricsListenerGaugeTest : RelayIntegrationTest() {

	@Autowired private lateinit var registry: MeterRegistry
	@Autowired private lateinit var metrics: RelayMetrics
	@Autowired private lateinit var listener: PgNotificationListener

	@Test
	fun `exposes the listener connection state`() {
		// SmartLifecycle.start() 는 리스너 연결을 백그라운드 스레드에서 시작하고 완료를 기다리지
		// 않는다. 컨텍스트 시작 직후 바로 단언하면 실제 DB 커넥션 + LISTEN 이 끝나기 전에
		// connected == false 를 읽을 위험이 있어 폴링으로 기다린다.
		val deadline = System.currentTimeMillis() + 10_000
		while (!listener.connected && System.currentTimeMillis() < deadline) {
			Thread.sleep(100)
		}
		metrics.refreshGauges()
		assertEquals(1.0, registry.get("relay.listener.connected").gauge().value())
	}
}
