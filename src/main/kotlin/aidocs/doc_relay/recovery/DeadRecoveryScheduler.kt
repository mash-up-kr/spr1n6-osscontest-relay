package aidocs.doc_relay.recovery

import aidocs.doc_relay.observability.RelayMetrics
import aidocs.doc_relay.outbox.OutboxRepository
import aidocs.doc_relay.signal.DrainTrigger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/**
 * spec §6-2. DEAD 를 PENDING 으로 되돌린다.
 *
 * 종착 상태가 없으므로 발행이 구조적으로 불가능한 행은 영원히 순환한다.
 * 순환은 relay.dead.transition.total 카운터와 WARN 로그로만 보이며,
 * 멈추는 수단은 어드민의 HOLD 다 (spec §6-3).
 */
@Component
class DeadRecoveryScheduler(
	private val repository: OutboxRepository,
	private val trigger: DrainTrigger,
	private val metrics: RelayMetrics,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	@Scheduled(fixedDelayString = "\${relay.dead.recovery-scan-interval}")
	fun recover(): Int {
		val recovered = repository.recoverDead()
		if (recovered > 0) {
			log.warn("DEAD 행 {}건을 PENDING 으로 자동 복구했다", recovered)
			metrics.recordDeadRecovery(recovered)
			trigger.signal()
		}
		return recovered
	}
}
