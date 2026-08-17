package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import aidocs.doc_relay.observability.RelayMetrics
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.stereotype.Component
import java.util.UUID

/**
 * spec §4. 한 사이클 = 선점(짧은 트랜잭션) → 발행(트랜잭션 밖) → 결과 반영.
 *
 * 중복 발행 지점: 발행 후 markPublished 커밋 전에 프로세스가 죽으면 그 행은
 * PUBLISHING 으로 남았다가 좀비 회수를 거쳐 다시 발행된다. at-least-once 계약상
 * 정상이며 워커 멱등성이 흡수한다. 없애려 하지 말 것 (spec §4).
 */
@Component
class OutboxDrainer(
	private val repository: OutboxRepository,
	private val publisher: KafkaPublisher,
	private val properties: RelayProperties,
	private val metrics: RelayMetrics,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	/** 한 사이클만 돈다. 선점한 건수를 돌려준다. */
	fun drainOnce(): Int {
		val claimed = repository.claimBatch(properties.drain.batchSize)
		if (claimed.isEmpty()) return 0
		metrics.recordBatchSize(claimed.size)

		val outcome = publisher.publish(claimed)

		repository.markPublished(outcome.succeeded)
		metrics.recordPublished(outcome.succeeded.size)
		val byId = claimed.associateBy { it.id }
		outcome.succeeded.forEach { id -> byId[id]?.let { metrics.recordLatency(it.createdAt) } }

		outcome.failed.forEach { (message, ids) ->
			withRowContext(ids, byId) { log.warn("발행 실패 {}건: {}", ids.size, message) }
			repository.markFailed(ids, message)
			metrics.recordPublishFailed(ids.size)
			// 이번 실패로 DEAD 가 된 행들을 센다. 순환 관측의 유일한 창이다.
			val deadIds = ids.filter { id ->
				(byId[id]?.publishAttemptCount ?: 0) + 1 >= properties.backoff.maxAttempts
			}
			if (deadIds.isNotEmpty()) {
				metrics.recordDeadTransition(deadIds.size)
				withRowContext(deadIds, byId) {
					log.warn("DEAD 전환 {}건. 마지막 에러: {}", deadIds.size, message)
				}
			}
		}
		return claimed.size
	}

	/**
	 * 로그 한 줄이 다루는 행(들)의 eventId/traceId 를 MDC 로 실어 grep 으로 상관지을 수 있게
	 * 한다 (spec §8). 이 서버는 HTTP 요청을 하나도 받지 않아 로그·메트릭이 유일한 디버깅
	 * 창이라 이 상관관계가 특히 중요하다. 묶음이 행 여러 개를 가리킬 수 있어(같은 에러
	 * 메시지를 공유하는 markFailed 배치) 쉼표로 이어 붙인다.
	 */
	private fun withRowContext(ids: List<UUID>, byId: Map<UUID, OutboxEventRow>, block: () -> Unit) {
		val eventId = ids.joinToString(",") { it.toString() }
		val traceId = ids.mapNotNull { byId[it]?.traceId }.distinct().joinToString(",")
		MDC.putCloseable("eventId", eventId).use {
			MDC.putCloseable("traceId", traceId).use {
				block()
			}
		}
	}

	/**
	 * 백로그가 남아 있으면 계속 돈다.
	 * 선점 건수가 batchSize 와 같다는 건 아직 남았다는 뜻이므로 다음 신호를 기다리지 않는다.
	 */
	fun drainUntilEmpty(): Int {
		var total = 0
		while (true) {
			val drained = drainOnce()
			total += drained
			if (drained < properties.drain.batchSize) return total
		}
	}
}
