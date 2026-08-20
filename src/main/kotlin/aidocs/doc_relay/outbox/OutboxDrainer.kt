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
		val byId = claimed.associateBy { it.id }
		val instanceId = properties.instanceId
		// 선점 SQL은 한 문장짜리 UPDATE라 now()가 한 번만 평가되고, 그 배치로 잡힌 행은
		// 전부 같은 locked_at을 갖는다. 그래서 첫 행 것만 꺼내 배치 전체의 소유권 표식으로 쓴다.
		val claimedAt = claimed.first().lockedAt

		val outcome = publisher.publish(claimed)

		markPublishedSafely(outcome.succeeded, instanceId, claimedAt, byId)

		outcome.failed.forEach { (group, ids) ->
			withRowContext(ids, byId) { log.warn("발행 실패 {}건: {}", ids.size, group.message) }
			if (group.permanent) {
				markDeadSafely(ids, group.message, instanceId, claimedAt)
				metrics.recordDeadTransition(ids.size)
				withRowContext(ids, byId) {
					log.warn("DEAD 전환(영구 실패) {}건. 사유: {}", ids.size, group.message)
				}
			} else {
				markFailedSafely(ids, group.message, instanceId, claimedAt)
				val deadIds = ids.filter { id ->
					(byId[id]?.publishAttemptCount ?: 0) + 1 >= properties.backoff.maxAttempts
				}
				if (deadIds.isNotEmpty()) {
					metrics.recordDeadTransition(deadIds.size)
					withRowContext(deadIds, byId) {
						log.warn("DEAD 전환 {}건. 마지막 에러: {}", deadIds.size, group.message)
					}
				}
			}
		}
		return claimed.size
	}

	/**
	 * 성공/실패 반영을 각각 독립적으로 실행한다. 하나가 DB 예외로 던져도 다른 하나는 그대로
	 * 돈다 — 순차 구조에서 앞이 던지면 뒤가 한 줄도 실행되지 않으면, 지표라는 유일한 관측
	 * 창까지 함께 닫힌다. `internal` 가시성은 이 독립성을 직접 테스트하기 위함이다.
	 */
	internal fun markPublishedSafely(
		ids: List<UUID>, instanceId: String, claimedAt: java.time.Instant, byId: Map<UUID, OutboxEventRow>,
	) {
		if (ids.isEmpty()) return
		try {
			val updated = repository.markPublished(ids, instanceId, claimedAt)
			reportStaleness(ids, updated)
			metrics.recordPublished(updated)
			ids.forEach { id -> byId[id]?.let { metrics.recordLatency(it.createdAt, it.publishAttemptCount == 0) } }
		} catch (e: Exception) {
			log.error("성공 반영 실패: {}건", ids.size, e)
			metrics.recordMarkFailure()
		}
	}

	internal fun markFailedSafely(ids: List<UUID>, message: String, instanceId: String, claimedAt: java.time.Instant) {
		try {
			val updated = repository.markFailed(ids, message, instanceId, claimedAt)
			reportStaleness(ids, updated)
			metrics.recordPublishFailed(updated)
		} catch (e: Exception) {
			log.error("실패 반영 실패: {}건", ids.size, e)
			metrics.recordMarkFailure()
		}
	}

	internal fun markDeadSafely(ids: List<UUID>, message: String, instanceId: String, claimedAt: java.time.Instant) {
		try {
			val updated = repository.markDead(ids, "PERMANENT: $message", instanceId, claimedAt)
			reportStaleness(ids, updated)
			metrics.recordPublishFailed(updated)
		} catch (e: Exception) {
			log.error("DEAD 반영 실패: {}건", ids.size, e)
			metrics.recordMarkFailure()
		}
	}

	private fun reportStaleness(ids: List<UUID>, updated: Int) {
		val stale = ids.size - updated
		if (stale > 0) {
			metrics.recordStaleWrite(stale)
			log.warn("소유권을 잃은 뒤늦은 결과 쓰기 {}건이 튕겨났다", stale)
		}
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
	 * 밀린 것이 없어질 때까지 사이클을 반복한다. [shouldStop] 이 true 를 돌려주면 아직 밀린
	 * 것이 남아 있어도 즉시 멈춘다 — 종료 신호를 사이클과 사이클 사이에서만 보면, 백로그가
	 * 클 때 신호를 오래 무시하게 된다.
	 */
	fun drainUntilEmpty(shouldStop: () -> Boolean = { false }): Int {
		var total = 0
		while (true) {
			val drained = drainOnce()
			total += drained
			if (drained < properties.drain.batchSize) return total
			if (shouldStop()) return total
		}
	}
}
