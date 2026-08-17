package aidocs.doc_relay.observability

/**
 * [held] 는 DEAD 중 next_attempt_at = 'infinity' 인 행이다.
 * status 가 DEAD 그대로라 조회 없이는 안 보이므로 따로 센다 — 사람이 멈춰놓고 잊는 일이 잦다.
 */
data class OutboxCounts(
	val pending: Int,
	val publishing: Int,
	val dead: Int,
	val held: Int,
)
