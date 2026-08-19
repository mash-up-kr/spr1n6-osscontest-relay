package aidocs.doc_relay.outbox

import java.util.UUID

/**
 * 배치를 발행하고 난 결과. 성공한 이벤트 목록과, 실패한 이벤트를 에러 메시지별로 묶은 것.
 *
 * 실패를 메시지별로 묶어 두는 이유는 DB 에 기록할 때 한 번의 UPDATE 로 접기 위해서다.
 * Kafka 가 통째로 죽으면 배치 전체가 같은 에러라 UPDATE 한 번으로 끝나고, 원인이 제각각일
 * 때만 그 가짓수만큼 나뉜다.
 */
data class PublishOutcome(
	val succeeded: List<UUID>,
	val failed: Map<String, List<UUID>>,
) {
	companion object {
		val EMPTY = PublishOutcome(emptyList(), emptyMap())
	}
}
