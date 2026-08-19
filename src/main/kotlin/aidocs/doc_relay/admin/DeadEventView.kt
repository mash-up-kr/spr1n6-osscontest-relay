package aidocs.doc_relay.admin

/**
 * 어드민 목록에 보여줄 이벤트 한 건.
 *
 * [held] 가 true 면 사람이 멈춰 둬서 자동 복구 대상에서 빠진 행이다. 멈춰 둔 행도 상태값은
 * 여전히 DEAD 라서, 이 플래그가 없으면 저절로 되살아날 것과 사람이 풀어 줘야 할 것이
 * 목록에서 똑같아 보인다.
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
