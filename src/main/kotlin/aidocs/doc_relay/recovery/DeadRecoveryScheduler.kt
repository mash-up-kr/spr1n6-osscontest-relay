package aidocs.doc_relay.recovery

import aidocs.doc_relay.observability.RelayMetrics
import aidocs.doc_relay.outbox.OutboxRepository
import aidocs.doc_relay.signal.DrainTrigger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/**
 * 재시도를 다 써서 포기했던 행을 시간이 지나면 다시 발행 대기 상태로 되살린다.
 * Kafka 가 오래 죽어 있었던 것은 이벤트 잘못이 아니므로, 사람 손을 빌리지 않고 회복시킨다.
 *
 * 대신 끝나는 상태가 없어진다. 아무리 다시 보내도 성공할 수 없는 이벤트는 실패와 부활을
 * 끝없이 반복한다. 그런 이벤트가 있다는 것은 relay.dead.transition.total 지표와 아래 경고
 * 로그로만 드러나고, 멈추는 방법은 운영자가 어드민에서 그 이벤트를 정지시키는 것뿐이다.
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
