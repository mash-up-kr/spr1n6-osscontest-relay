package aidocs.doc_relay.observability

import aidocs.doc_relay.outbox.OutboxRepository
import aidocs.doc_relay.signal.PgNotificationListener
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.context.annotation.Lazy
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * 릴레이의 상태를 지표로 내보낸다. 이 서버는 HTTP 요청을 받지 않아서, 여기서 내보내는 값과
 * 로그가 밖에서 상태를 볼 수 있는 전부다.
 *
 * 이벤트 식별자를 지표 태그로 쓰지 않는다. 태그 값의 가짓수만큼 시계열이 만들어지므로,
 * 이벤트마다 다른 값을 붙이면 지표 저장소가 감당하지 못한다.
 *
 * 눈여겨볼 지표는 relay.dead.transition.total 이다. 이 값이 발행 성공 건수보다 빠르게 오르면
 * 같은 이벤트가 계속 실패하고 되살아나기를 반복하고 있다는 뜻이다. 자동 복구가 재시도 횟수를
 * 0으로 되돌리기 때문에 DB 만 봐서는 알 수 없고, 이 지표가 유일한 단서다.
 *
 * 리스너를 [Lazy] 로 받는 이유: 이 클래스는 드레이너가 생성자로 참조하는데, 리스너는 다시
 * 드레이너 쪽을 거쳐 이 클래스에 닿는 고리가 있다. 그대로 두면 스프링이 어느 쪽도 먼저
 * 완성하지 못한다. 실제로 리스너를 쓰는 시점은 아래 주기 갱신뿐이라 지연 참조로 충분하다.
 */
@Component
class RelayMetrics(
	registry: MeterRegistry,
	private val repository: OutboxRepository,
	@Lazy private val listener: PgNotificationListener,
) {

	private val publishSuccess: Counter = registry.counter("relay.publish.total", "result", "success")
	private val publishFailure: Counter = registry.counter("relay.publish.total", "result", "failure")
	private val deadTransition: Counter = registry.counter("relay.dead.transition.total")
	private val deadRecovery: Counter = registry.counter("relay.dead.recovery.total")
	private val zombieReclaim: Counter = registry.counter("relay.zombie.reclaim.total")
	private val reconnects: Counter = registry.counter("relay.listener.reconnect.total")
	private val latency: Timer = registry.timer("relay.publish.latency")
	private val batchSize: DistributionSummary = registry.summary("relay.drain.batch.size")

	private val pendingGauge = AtomicInteger(0)
	private val deadGauge = AtomicInteger(0)
	private val heldGauge = AtomicInteger(0)
	private val connectedGauge = AtomicInteger(0)
	private var lastReconnectCount = 0L

	init {
		registry.gauge("relay.outbox.pending", pendingGauge) { it.get().toDouble() }
		registry.gauge("relay.outbox.dead", deadGauge) { it.get().toDouble() }
		registry.gauge("relay.outbox.held", heldGauge) { it.get().toDouble() }
		registry.gauge("relay.listener.connected", connectedGauge) { it.get().toDouble() }
	}

	fun recordPublished(count: Int) = publishSuccess.increment(count.toDouble())
	fun recordPublishFailed(count: Int) = publishFailure.increment(count.toDouble())
	fun recordDeadTransition(count: Int) = deadTransition.increment(count.toDouble())
	fun recordDeadRecovery(count: Int) = deadRecovery.increment(count.toDouble())
	fun recordZombieReclaim(count: Int) = zombieReclaim.increment(count.toDouble())
	fun recordBatchSize(size: Int) = batchSize.record(size.toDouble())

	/**
	 * 행이 만들어진 순간부터 발행이 끝난 지금까지의 시간을 기록한다. 릴레이가 발행에 쓴 시간이
	 * 아니라 사용자가 업로드한 뒤 워커에게 도착하기까지의 전체 지연이다.
	 */
	fun recordLatency(createdAt: Instant) = latency.record(Duration.between(createdAt, Instant.now()))

	/**
	 * 상태별 건수를 주기적으로 한 번의 쿼리로 갱신한다. 지표를 수집해 갈 때마다 세면
	 * 수집 주기가 짧아질수록 DB 부하가 그대로 따라 올라간다.
	 */
	@Scheduled(fixedDelayString = "\${relay.metrics.gauge-refresh-interval}")
	fun refreshGauges() {
		val counts = repository.counts()
		pendingGauge.set(counts.pending)
		deadGauge.set(counts.dead)
		heldGauge.set(counts.held)
		connectedGauge.set(if (listener.connected) 1 else 0)

		val current = listener.reconnectCount
		if (current > lastReconnectCount) {
			reconnects.increment((current - lastReconnectCount).toDouble())
			lastReconnectCount = current
		}
	}
}
