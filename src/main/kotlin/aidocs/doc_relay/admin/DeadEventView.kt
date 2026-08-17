package aidocs.doc_relay.admin

/**
 * [held] 는 next_attempt_at = 'infinity' 라 자동 복구에서 제외된 행이다.
 * status 는 DEAD 그대로라 이 플래그 없이는 정지 여부가 안 보인다.
 */
data class DeadEventView(
	val eventId: String,
	val documentId: Long,
	val publishAttemptCount: Int,
	val nextAttemptAt: String,
	val held: Boolean,
	val lastErrorMessage: String?,
	val createdAt: String,
)
