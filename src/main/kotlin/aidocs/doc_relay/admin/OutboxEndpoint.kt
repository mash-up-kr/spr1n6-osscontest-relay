package aidocs.doc_relay.admin

import aidocs.doc_relay.RelayProperties
import aidocs.doc_relay.observability.OutboxCounts
import aidocs.doc_relay.observability.RelayMetrics
import aidocs.doc_relay.outbox.OutboxRepository
import aidocs.doc_relay.signal.DrainTrigger
import org.slf4j.LoggerFactory
import org.springframework.boot.actuate.endpoint.annotation.Endpoint
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation
import org.springframework.boot.actuate.endpoint.annotation.Selector
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation
import org.springframework.stereotype.Component
import java.security.MessageDigest
import java.util.UUID

/**
 * 운영자가 밖에서 상태를 보고 손댈 수 있는 창구. 발행이 막힌 이벤트를 확인하고, 다시 보내거나
 * 멈추거나 다시 풀 수 있다.
 *
 * 일반 컨트롤러가 아니라 액추에이터 엔드포인트로 만들었다. 이 서버는 요청을 받지 않는다는
 * 구조를 유지하기 위해서다. 애플리케이션 포트는 닫혀 있고, 이 엔드포인트는 관리 포트에서
 * 로컬 주소로만 열린다.
 *
 * 네 동작 모두 기존 행을 UPDATE 할 뿐 새 행을 만들지 않는다. "릴레이는 행을 만들지 않는다"는
 * 규칙은 운영자 조작에도 그대로 적용된다.
 */
@Component
@Endpoint(id = "outbox")
class OutboxEndpoint(
	private val repository: OutboxRepository,
	private val trigger: DrainTrigger,
	private val metrics: RelayMetrics,
	private val properties: RelayProperties,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	/** `GET /actuator/outbox` — 상태별 건수를 돌려준다. */
	@ReadOperation
	fun summary(): OutboxCounts = repository.counts()

	/** `GET /actuator/outbox/dead` — 발행을 포기한 이벤트 목록. 멈춰 둔 것도 함께 나온다. */
	@ReadOperation
	fun byStatus(@Selector status: String): List<DeadEventView> {
		require(status.equals("dead", ignoreCase = true)) {
			"조회 가능한 상태는 'dead' 뿐이다. 받은 값: $status"
		}
		return repository.findDead(DEAD_LIST_LIMIT)
	}

	/**
	 * `POST /actuator/outbox/{id}` — 이벤트 하나를 조작한다.
	 * 본문은 `{"action": ...}` 형태다.
	 *
	 *   REPUBLISH        — 자동 복구를 기다리지 않고 지금 바로 다시 발행한다 (DEAD 행 대상)
	 *   HOLD             — 자동 복구 대상에서 빼서 순환을 멈춘다 (DEAD 행 대상)
	 *   RELEASE          — 멈춰 둔 것을 다시 자동 복구 대상으로 되돌린다 (DEAD 행 대상)
	 *   FORCE_REPUBLISH  — 이미 끝난 발행을 강제로 되돌린다 (PUBLISHED 행 대상, 파괴적)
	 */
	@WriteOperation
	fun act(@Selector id: String, action: String, token: String? = null) {
		val configuredToken = properties.admin.token
		if (configuredToken.isNotBlank() &&
			!MessageDigest.isEqual(
				(token ?: "").toByteArray(Charsets.UTF_8),
				configuredToken.toByteArray(Charsets.UTF_8),
			)
		) {
			throw SecurityException("어드민 쓰기 액션 인증 실패: eventId=$id, action=$action")
		}

		val eventId = runCatching { UUID.fromString(id) }
			.getOrElse { throw IllegalArgumentException("이벤트 id 가 UUID 가 아니다: $id") }

		val updated = when (action.uppercase()) {
			"REPUBLISH" -> repository.republish(eventId).also { trigger.signal() }
			"HOLD" -> repository.hold(eventId)
			"RELEASE" -> repository.release(eventId)
			"FORCE_REPUBLISH" -> repository.forceRepublish(eventId).also {
				if (it > 0) {
					trigger.signal()
					metrics.recordForcedRepublish()
					log.warn("어드민 FORCE_REPUBLISH 적용(파괴적): eventId={}", eventId)
				}
			}
			else -> throw IllegalArgumentException(
				"알 수 없는 action: $action. REPUBLISH / HOLD / RELEASE / FORCE_REPUBLISH 중 하나여야 한다."
			)
		}
		log.info("어드민 {} 적용: eventId={}, 갱신 {}행", action.uppercase(), eventId, updated)
	}

	private companion object {
		const val DEAD_LIST_LIMIT = 200
	}
}
