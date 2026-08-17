package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import aidocs.doc_relay.admin.DeadEventView
import aidocs.doc_relay.observability.OutboxCounts
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * outbox_event 에 대한 SQL 을 독점한다. 다른 클래스는 SQL 문자열을 갖지 않는다.
 *
 * 릴레이는 이 테이블에 INSERT/DELETE 하지 않는다 (spec §2). 모든 쓰기는 UPDATE 다.
 */
@Repository
class OutboxRepository(
	private val jdbc: JdbcClient,
	private val properties: RelayProperties,
	private val backoffPolicy: BackoffPolicy,
) {

	/**
	 * PENDING 이고 시간이 된 행을 최대 [limit] 건 선점해 PUBLISHING 으로 바꾸고 돌려준다.
	 *
	 * CTE + RETURNING 한 문장이라 단일 트랜잭션으로 원자적이며, 락을 문 채로
	 * 네트워크 IO 를 하는 구간이 존재할 수 없다 (spec §4). SELECT 다음 UPDATE 를 별도
	 * 문장으로 나누면 그 사이에 락을 놓쳐 경합이 생기거나, 락을 쥔 채 Kafka 로 나가는
	 * 문이 열리게 된다 — 한 문장으로 묶는 이유가 바로 그것이다.
	 * ORDER BY 가 idx_outbox_pending (next_attempt_at, id) 순서와 일치해 정렬이 붙지 않는다.
	 */
	fun claimBatch(limit: Int): List<OutboxEventRow> =
		jdbc.sql(CLAIM_SQL)
			.param("limit", limit)
			.param("instanceId", properties.instanceId)
			.query(ROW_MAPPER)
			.list()

	/** Kafka ack 를 받은 행들을 한 번에 PUBLISHED 로 내린다. */
	fun markPublished(ids: List<UUID>): Int {
		if (ids.isEmpty()) return 0
		return jdbc.sql(MARK_PUBLISHED_SQL)
			.param("ids", ids)
			.update()
	}

	/**
	 * 발행에 실패한 행들을 반영한다. 같은 에러 메시지를 공유하는 묶음을 한 문장으로 처리한다.
	 *
	 * 증가 후 카운트가 maxAttempts 에 도달하면 DEAD 로, 아니면 PENDING + 백오프로 간다.
	 * 백오프는 행마다 publish_attempt_count 가 달라 SQL 식으로 계산한다 (spec §4③).
	 */
	fun markFailed(ids: List<UUID>, message: String): Int {
		if (ids.isEmpty()) return 0
		return jdbc.sql(MARK_FAILED_SQL)
			.param("ids", ids)
			.param("message", message)
			.param("maxAttempts", properties.backoff.maxAttempts)
			.param("baseSeconds", backoffPolicy.baseSeconds)
			.param("maxSeconds", backoffPolicy.maxSeconds)
			.param("deadRecoveryDelaySeconds", properties.dead.recoveryDelay.toMillis() / 1000.0)
			.update()
	}

	/**
	 * PUBLISHING 인 채로 락이 만료된 행을 PENDING 으로 되돌린다 (spec §6-1).
	 *
	 * 카운트를 올린다. 올리지 않으면 릴레이를 반복해서 죽이는 행이 회수 <-> 재시도를
	 * 무한 반복하며 DEAD 에 영원히 도달하지 못한다.
	 *
	 * 항상 PENDING 으로만 보낸다. 카운트가 임계치를 넘었어도 여기서 DEAD 로 내리지 않는다 —
	 * 회수 시점은 락이 만료됐다는 사실만 아는 시점이고, 릴레이가 배포로 재시작됐거나
	 * GC 로 잠깐 멈춘 경우도 여기 걸린다.
	 */
	fun reclaimZombies(): Int =
		jdbc.sql(RECLAIM_ZOMBIES_SQL)
			.param("lockTimeoutSeconds", properties.zombie.lockTimeout.toMillis() / 1000.0)
			.param("baseSeconds", backoffPolicy.baseSeconds)
			.param("maxSeconds", backoffPolicy.maxSeconds)
			.update()

	/**
	 * DEAD 는 종착역이 아니다 (spec §6-2). 복구 지연이 지난 행을 PENDING 으로 되돌리고
	 * 카운터를 0으로 리셋한다. 새 행을 만들지 않고 같은 행을 그대로 쓴다.
	 *
	 * next_attempt_at 이 'infinity' 인 행은 여기서 자연스럽게 빠진다 — 정지 스위치다.
	 */
	fun recoverDead(): Int =
		jdbc.sql(RECOVER_DEAD_SQL).update()

	/**
	 * 순환하는 독성 메시지를 멈춘다. 스키마 변경 없이 복구 대상에서 제외된다 (spec §6-3).
	 *
	 * DEAD 행에만 적용된다. 상태를 확인하지 않으면 PENDING 행도 잘못 정지시킬 수 있는데,
	 * 그렇게 정지된 행은 held 게이지에도 findDead() 목록에도 안 잡히면서 영원히 발행되지
	 * 않는다 — held 게이지가 막으려던 바로 그 실패 모드라 어드민 워크플로우가 실제로
	 * 다루는 대상(DEAD 목록)으로 쓰기 범위를 좁힌다.
	 */
	fun hold(id: UUID): Int =
		jdbc.sql(HOLD_SQL).param("id", id).update()

	/** 정지를 해제한다. 다음 복구 스캔에서 다시 잡힌다. DEAD 행에만 적용된다 (위 hold() 참고). */
	fun release(id: UUID): Int =
		jdbc.sql(RELEASE_SQL).param("id", id).update()

	/**
	 * 어드민 강제 재발행. 기존 행을 되돌릴 뿐 새 행을 만들지 않는다 (spec §2).
	 * DEAD 행에만 적용된다 (위 hold() 참고).
	 */
	fun republish(id: UUID): Int =
		jdbc.sql(REPUBLISH_SQL).param("id", id).update()

	/**
	 * 게이지 갱신용 집계. 네 스칼라 서브쿼리로 나눠 PENDING/PUBLISHING 은 각각
	 * idx_outbox_pending/idx_outbox_stuck 부분 인덱스를 타게 한다 — 두 서브쿼리의
	 * WHERE 조건이 해당 인덱스의 부분 조건과 정확히 일치해야 플래너가 인덱스만 스캔한다.
	 * DEAD/held 집계는 부분 인덱스가 없어 여전히 Seq Scan 이다 (spec §6-2). M5 에서 실측한다.
	 */
	fun counts(): OutboxCounts =
		jdbc.sql(
			"""
			SELECT
			    (SELECT count(*) FROM outbox_event WHERE status = 'PENDING')                                  AS pending,
			    (SELECT count(*) FROM outbox_event WHERE status = 'PUBLISHING')                               AS publishing,
			    (SELECT count(*) FROM outbox_event WHERE status = 'DEAD' AND next_attempt_at <> 'infinity')    AS dead,
			    (SELECT count(*) FROM outbox_event WHERE status = 'DEAD' AND next_attempt_at =  'infinity')    AS held
			""".trimIndent()
		).query { rs, _ ->
			OutboxCounts(
				pending = rs.getInt("pending"),
				publishing = rs.getInt("publishing"),
				dead = rs.getInt("dead"),
				held = rs.getInt("held"),
			)
		}.single()

	/** 어드민 DEAD 목록. 정지된 행도 [DeadEventView.held] 로 함께 보여준다. */
	fun findDead(limit: Int): List<DeadEventView> =
		jdbc.sql(
			"""
			SELECT id, document_id, publish_attempt_count, next_attempt_at,
			       (next_attempt_at = 'infinity') AS held,
			       last_error_message, created_at
			  FROM outbox_event
			 WHERE status = 'DEAD'
			 ORDER BY created_at DESC
			 LIMIT :limit
			""".trimIndent()
		).param("limit", limit).query { rs, _ ->
			DeadEventView(
				eventId = rs.getObject("id", UUID::class.java).toString(),
				documentId = rs.getLong("document_id"),
				publishAttemptCount = rs.getInt("publish_attempt_count"),
				nextAttemptAt = if (rs.getBoolean("held")) "infinity"
					else DateTimeFormatter.ISO_INSTANT.format(rs.getTimestamp("next_attempt_at").toInstant()),
				held = rs.getBoolean("held"),
				lastErrorMessage = rs.getString("last_error_message"),
				createdAt = DateTimeFormatter.ISO_INSTANT.format(rs.getTimestamp("created_at").toInstant()),
			)
		}.list()

	private companion object {
		val ROW_MAPPER = RowMapper { rs, _ ->
			OutboxEventRow(
				id = rs.getObject("id", UUID::class.java),
				tenantId = rs.getLong("tenant_id"),
				documentId = rs.getLong("document_id"),
				documentVersionId = rs.getLong("document_version_id"),
				eventType = rs.getString("event_type"),
				eventSchemaVersion = rs.getInt("event_schema_version"),
				payload = rs.getString("payload"),
				traceId = rs.getString("trace_id"),
				publishAttemptCount = rs.getInt("publish_attempt_count"),
				createdAt = rs.getTimestamp("created_at").toInstant(),
			)
		}

		val CLAIM_SQL = """
			WITH claimed AS (
			    SELECT id
			      FROM outbox_event
			     WHERE status = 'PENDING' AND next_attempt_at <= now()
			     ORDER BY next_attempt_at, id
			     LIMIT :limit
			       FOR UPDATE SKIP LOCKED
			)
			UPDATE outbox_event o
			   SET status = 'PUBLISHING',
			       locked_by = :instanceId,
			       locked_at = now()
			  FROM claimed c
			 WHERE o.id = c.id
			RETURNING o.id, o.tenant_id, o.document_id, o.document_version_id,
			          o.event_type, o.event_schema_version, o.payload::text AS payload,
			          o.trace_id, o.publish_attempt_count, o.created_at
			""".trimIndent()

		// PostgreSQL JDBC 드라이버는 UUID[] 를 uuid[] 로 자동 변환하지 않아 = ANY(:ids) 가
		// 조용히 매칭에 실패할 수 있다. JdbcClient 는 NamedParameterJdbcTemplate 기반이라
		// Collection 을 IN (:ids) 에 바인딩하면 IN (?, ?, ...) 로 펼쳐 준다.
		val MARK_PUBLISHED_SQL = """
			UPDATE outbox_event
			   SET status = 'PUBLISHED',
			       published_at = now(),
			       locked_by = NULL,
			       locked_at = NULL
			 WHERE id IN (:ids)
			""".trimIndent()

		// BackoffPolicy.SQL_EXPRESSION 의 :attemptCount 자리에 컬럼을 끼워 넣는다.
		val MARK_FAILED_SQL = """
			UPDATE outbox_event
			   SET publish_attempt_count = publish_attempt_count + 1,
			       status = CASE
			           WHEN publish_attempt_count + 1 >= :maxAttempts THEN 'DEAD'
			           ELSE 'PENDING'
			       END,
			       next_attempt_at = now() + CASE
			           WHEN publish_attempt_count + 1 >= :maxAttempts THEN :deadRecoveryDelaySeconds
			           ELSE ${BackoffPolicy.SQL_EXPRESSION.replace(":attemptCount", "publish_attempt_count")}
			       END * INTERVAL '1 second',
			       last_error_message = :message,
			       locked_by = NULL,
			       locked_at = NULL
			 WHERE id IN (:ids)
			""".trimIndent()

		// BackoffPolicy.SQL_EXPRESSION 의 :attemptCount 자리에 컬럼을 끼워 넣는다.
		val RECLAIM_ZOMBIES_SQL = """
			UPDATE outbox_event
			   SET status = 'PENDING',
			       publish_attempt_count = publish_attempt_count + 1,
			       next_attempt_at = now() +
			           ${BackoffPolicy.SQL_EXPRESSION.replace(":attemptCount", "publish_attempt_count")}
			           * INTERVAL '1 second',
			       last_error_message = 'reclaimed: publishing lock timeout',
			       locked_by = NULL,
			       locked_at = NULL
			 WHERE status = 'PUBLISHING'
			   AND locked_at < now() - (:lockTimeoutSeconds * INTERVAL '1 second')
			""".trimIndent()

		val RECOVER_DEAD_SQL = """
			UPDATE outbox_event
			   SET status = 'PENDING', publish_attempt_count = 0, next_attempt_at = now()
			 WHERE status = 'DEAD' AND next_attempt_at <= now()
			""".trimIndent()

		val HOLD_SQL =
			"UPDATE outbox_event SET next_attempt_at = 'infinity' WHERE id = :id AND status = 'DEAD'"

		val RELEASE_SQL =
			"UPDATE outbox_event SET next_attempt_at = now() WHERE id = :id AND status = 'DEAD'"

		val REPUBLISH_SQL = """
			UPDATE outbox_event
			   SET status = 'PENDING', publish_attempt_count = 0, next_attempt_at = now(),
			       locked_by = NULL, locked_at = NULL
			 WHERE id = :id AND status = 'DEAD'
			""".trimIndent()
	}
}
