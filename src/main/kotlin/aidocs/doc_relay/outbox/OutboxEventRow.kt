package aidocs.doc_relay.outbox

import java.time.Instant
import java.util.UUID

/**
 * 발행하려고 집어 온 outbox_event 행 하나. 메시지를 만드는 데 필요한 컬럼만 담는다.
 *
 * payload 는 해석하지 않고 DB 에서 읽은 문자열 그대로 들고 다닌다. 릴레이는 그 안에 무엇이
 * 들어 있는지 알 필요가 없다.
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
	/** 이 행을 잡은 사이클의 표식. 결과 쓰기가 "지금 이 사이클이 여전히 주인인가"를 확인하는 데 쓴다. */
	val lockedAt: Instant,
)
