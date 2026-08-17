package aidocs.doc_relay.outbox

import java.time.Instant
import java.util.UUID

/**
 * 선점한 outbox_event 행. 봉투 조립에 필요한 컬럼만 담는다.
 *
 * payload 는 파싱하지 않고 JSONB 원문 문자열 그대로 들고 다닌다 (spec §7).
 */
data class OutboxEventRow(
	val id: UUID,
	val tenantId: Long,
	val documentId: Long,
	val documentVersionId: Long,
	val eventType: String,
	val eventSchemaVersion: Int,
	val payload: String,
	val traceId: String?,
	val publishAttemptCount: Int,
	val createdAt: Instant,
)
