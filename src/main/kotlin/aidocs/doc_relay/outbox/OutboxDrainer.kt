package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import aidocs.doc_relay.observability.RelayMetrics
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.stereotype.Component
import java.util.UUID

/**
 * 발행 사이클을 조립한다. 한 사이클은 세 단계다.
 *
 *   1. 선점 — 발행할 행을 집어 내 것으로 표시한다. 짧은 트랜잭션이고 네트워크를 쓰지 않는다.
 *   2. 발행 — Kafka 로 보낸다. 트랜잭션 밖이라 DB 락을 쥔 채 네트워크를 기다리지 않는다.
 *   3. 반영 — 성공과 실패를 DB 에 기록한다.
 *
 * 같은 이벤트가 두 번 발행될 수 있는 지점이 하나 있다. Kafka 가 받았다는 응답까지 왔는데
 * 3단계를 커밋하기 전에 프로세스가 죽으면, 그 행은 PUBLISHING 으로 남아 있다가 좀비 회수를
 * 거쳐 다시 발행된다. 이것은 고쳐야 할 버그가 아니라 이 서버가 내건 보장(at-least-once)의
 * 결과다. 받는 쪽이 같은 이벤트를 두 번 받아도 문제없게 만들어 두었으므로 여기서 없애려
 * 하지 않는다. 없애려면 DB 트랜잭션과 Kafka 트랜잭션을 묶어야 하는데 그럴 가치가 없다.
 */
@Component
class OutboxDrainer(
	private val repository: OutboxRepository,
	private val publisher: KafkaPublisher,
	private val properties: RelayProperties,
	private val metrics: RelayMetrics,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	/** 사이클을 한 번만 돈다. 선점한 건수를 돌려준다. */
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
			// 이번 실패로 DEAD 가 된 행이 몇 건인지 센다. 자동 복구가 재시도 횟수를 0으로
			// 되돌리기 때문에 DB 만 봐서는 몇 바퀴째 돌고 있는지 알 수 없고, 이 지표가
			// 같은 이벤트가 계속 순환하고 있다는 것을 알 수 있는 유일한 수단이다.
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
	 * 로그 한 줄이 어떤 이벤트를 가리키는지 남긴다. 이 서버는 HTTP 요청을 하나도 받지 않아서
	 * 로그와 지표가 유일한 디버깅 수단이고, 이벤트 식별자가 없으면 나중에 특정 문서의 발행이
	 * 왜 늦었는지 추적할 방법이 없다.
	 *
	 * 로그 한 줄이 여러 행을 한꺼번에 가리킬 수 있어서(같은 에러 메시지를 공유하는 실패 묶음)
	 * 식별자를 쉼표로 이어 붙인다.
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
	 * 밀린 것이 없어질 때까지 사이클을 반복한다.
	 *
	 * 한 번에 집어 오는 상한만큼 꽉 채워 왔다면 아직 남아 있다는 뜻이므로, 다음 신호를
	 * 기다리지 않고 바로 다시 돈다. 상한보다 적게 왔으면 다 비운 것이다.
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
