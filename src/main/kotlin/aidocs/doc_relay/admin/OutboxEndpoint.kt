package aidocs.doc_relay.admin

import aidocs.doc_relay.observability.OutboxCounts
import aidocs.doc_relay.outbox.OutboxRepository
import aidocs.doc_relay.signal.DrainTrigger
import org.slf4j.LoggerFactory
import org.springframework.boot.actuate.endpoint.annotation.Endpoint
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation
import org.springframework.boot.actuate.endpoint.annotation.Selector
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation
import org.springframework.stereotype.Component
import java.util.UUID

/**
 * spec §8. 어드민 조작. @RestController 가 아니라 Actuator 엔드포인트다 —
 * 이 서버는 요청을 받지 않는다는 구조를 유지하고, management 포트에서 127.0.0.1 에만 바인딩된다.
 *
 * 세 액션 모두 기존 행 UPDATE 다. 어드민 경로도 "릴레이는 INSERT 하지 않는다" 를 지킨다 (spec §2).
 */
@Component
@Endpoint(id = "outbox")
class OutboxEndpoint(
	private val repository: OutboxRepository,
	private val trigger: DrainTrigger,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	/** GET /actuator/outbox */
	@ReadOperation
	fun summary(): OutboxCounts = repository.counts()

	/** GET /actuator/outbox/dead */
	@ReadOperation
	fun byStatus(@Selector status: String): List<DeadEventView> {
		require(status.equals("dead", ignoreCase = true)) {
			"조회 가능한 상태는 'dead' 뿐이다. 받은 값: $status"
		}
		return repository.findDead(DEAD_LIST_LIMIT)
	}

	/** POST /actuator/outbox/{id}  body: {"action": "REPUBLISH" | "HOLD" | "RELEASE"} */
	@WriteOperation
	fun act(@Selector id: String, action: String) {
		val eventId = runCatching { UUID.fromString(id) }
			.getOrElse { throw IllegalArgumentException("이벤트 id 가 UUID 가 아니다: $id") }

		val updated = when (action.uppercase()) {
			"REPUBLISH" -> repository.republish(eventId).also { trigger.signal() }
			"HOLD" -> repository.hold(eventId)
			"RELEASE" -> repository.release(eventId)
			else -> throw IllegalArgumentException(
				"알 수 없는 action: $action. REPUBLISH / HOLD / RELEASE 중 하나여야 한다."
			)
		}
		log.info("어드민 {} 적용: eventId={}, 갱신 {}행", action.uppercase(), eventId, updated)
	}

	private companion object {
		const val DEAD_LIST_LIMIT = 200
	}
}
