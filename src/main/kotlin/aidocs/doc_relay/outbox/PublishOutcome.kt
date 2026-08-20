package aidocs.doc_relay.outbox

import java.util.UUID

/**
 * 배치를 발행하고 난 결과. 성공한 이벤트 목록과, 실패한 이벤트를 [FailureGroup](메시지 +
 * 영구/일시 분류)별로 묶은 것.
 */
data class PublishOutcome(
	val succeeded: List<UUID>,
	val failed: Map<FailureGroup, List<UUID>>,
) {
	companion object {
		val EMPTY = PublishOutcome(emptyList(), emptyMap())
	}
}
