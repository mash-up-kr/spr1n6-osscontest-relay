package aidocs.doc_relay.outbox

import java.util.UUID

/**
 * 배치 발행 결과.
 *
 * [failed] 를 에러 메시지로 묶어 두는 이유는 markFailed 가 메시지 단위로 한 문장씩
 * 접히게 하기 위해서다. 카프카가 죽으면 배치 전체가 같은 메시지라 UPDATE 한 번으로 끝난다.
 */
data class PublishOutcome(
	val succeeded: List<UUID>,
	val failed: Map<String, List<UUID>>,
) {
	val total: Int get() = succeeded.size + failed.values.sumOf { it.size }

	companion object {
		val EMPTY = PublishOutcome(emptyList(), emptyMap())
	}
}
