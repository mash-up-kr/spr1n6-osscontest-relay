package aidocs.doc_relay.recovery

import aidocs.doc_relay.observability.RelayMetrics
import aidocs.doc_relay.outbox.OutboxRepository
import aidocs.doc_relay.signal.DrainTrigger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/**
 * 발행 도중에 릴레이가 죽어 PUBLISHING 인 채로 남은 행을 주기적으로 되돌린다.
 * 이것이 없으면 그런 행은 아무도 다시 집지 않아 영영 발행되지 않는다.
 *
 * 되돌릴 행이 있었다면 드레인을 깨워 다음 주기를 기다리지 않게 한다.
 * 판단과 갱신은 전부 SQL 한 문장이 하고, 이 클래스는 주기만 챙긴다.
 */
@Component
class ZombieRecoveryScheduler(
	private val repository: OutboxRepository,
	private val trigger: DrainTrigger,
	private val metrics: RelayMetrics,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	@Scheduled(fixedDelayString = "\${relay.zombie.scan-interval}")
	fun reclaim(): Int {
		val reclaimed = repository.reclaimZombies()
		if (reclaimed > 0) {
			log.warn("PUBLISHING 으로 버려진 행 {}건을 PENDING 으로 회수했다", reclaimed)
			metrics.recordZombieReclaim(reclaimed)
			trigger.signal()
		}
		return reclaimed
	}
}
