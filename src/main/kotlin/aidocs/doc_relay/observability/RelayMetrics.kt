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
 * spec §8. eventId 를 태그로 쓰지 않는다 — 카디널리티가 터진다.
 *
 * relay.dead.transition.total 이 relay.publish.total{result=success} 보다 빠르게 오르면
 * 순환 중이라는 신호다. publish_attempt_count 를 리셋하기 때문에 DB 로는 못 보는 값이라,
 * 이 카운터가 유일한 관측 창이다.
 *
 * listener 는 @Lazy 로 받는다: RelayMetrics -> PgNotificationListener -> DrainTrigger ->
 * OutboxDrainer -> RelayMetrics 로 이어지는 순환 의존이 있어(OutboxDrainer 가 이 클래스를
 * 생성자로 받는다), 즉시 주입하면 컨테이너가 어느 쪽도 먼저 완성하지 못한다. 실제 사용은
 * refreshGauges() 스케줄 콜백 시점뿐이라 지연 프록시로 충분하다.
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

	/** created_at -> 지금. 파이프라인 전체 지연이다. */
	fun recordLatency(createdAt: Instant) = latency.record(Duration.between(createdAt, Instant.now()))

	/** 스크레이핑마다 세면 부담이라 주기적으로 한 번의 쿼리로 갱신한다. */
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
