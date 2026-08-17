package aidocs.doc_relay.recovery

import aidocs.doc_relay.observability.RelayMetrics
import aidocs.doc_relay.outbox.OutboxRepository
import aidocs.doc_relay.signal.DrainTrigger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/** spec §6-1. 리포지토리 호출 한 번뿐인 얇은 껍데기다. */
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
