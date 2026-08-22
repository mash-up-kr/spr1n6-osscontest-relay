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
	/**
	 * 이벤트 종류에 따라 없을 수 있다. INDEXING_REQUESTED 는 버전 단위라 값을 가지고,
	 * DOCUMENT_DELETED 는 문서 단위라 NULL 이다 — 스키마의 ck_outbox_*_target 이 이 관계를 강제한다.
	 *
	 * nullable 이어야 하는 이유가 하나 더 있다. NOT NULL 로 두면 JDBC 의 getLong 이 SQL NULL 을
	 * 0 으로 돌려주는 탓에(getLong 은 NULL 에 예외를 내지 않는다) 없는 값이 0 번 버전으로 둔갑해
	 * 오류 없이 그대로 발행된다.
	 */
	val documentVersionId: Long?,
	val eventType: String,
	val eventSchemaVersion: Int,
	val payload: String,
	val traceId: String?,
	val publishAttemptCount: Int,
	val createdAt: Instant,
	/** 이 행을 잡은 사이클의 표식. 결과 쓰기가 "지금 이 사이클이 여전히 주인인가"를 확인하는 데 쓴다. */
	val lockedAt: Instant,
)
