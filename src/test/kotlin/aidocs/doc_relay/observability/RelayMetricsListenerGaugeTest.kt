package aidocs.doc_relay.observability

import aidocs.doc_relay.signal.PgNotificationListener
import aidocs.doc_relay.support.RelayIntegrationTest
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertEquals

// 리스너를 켜는 유일한 클래스라 따로 뗀다. @TestPropertySource 는 클래스 단위로 적용되므로
// RelayMetricsTest 에 같이 두면 그 테스트들의 insertVersion() 이 진짜 pg_notify 를 쏘고,
// 그때 도는 DrainTrigger 가 명시적 drainOnce() 와 경합한다.
//
// 클래스를 나누는 것만으로는 부족하다. 컨텍스트가 캐시에 남으면 여기서 켠 PgNotificationListener
// 와 DrainTrigger(둘 다 SmartLifecycle)가 JVM 종료까지 살아, 뒤이어 도는 다른 클래스가 공유 DB 에
// 쏘는 pg_notify 에까지 반응한다. @DirtiesContext(AFTER_CLASS) 로 컨텍스트를 닫아 stop() 을
// 부른다. postgres/kafka 는 Spring 밖의 싱글턴이라 그대로 공유된다.
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
