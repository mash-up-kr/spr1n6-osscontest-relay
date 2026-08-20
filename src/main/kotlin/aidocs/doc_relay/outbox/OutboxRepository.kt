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
 * outbox_event 에 대한 모든 SQL 이 여기 모여 있다. 다른 클래스는 SQL 문자열을 갖지 않는다.
 *
 * 이 테이블에 INSERT 도 DELETE 도 하지 않는다. 모든 쓰기가 UPDATE 다. 행을 만드는 것은
 * API 서버 쪽 DB 트리거이고, 지우는 것은 이 서버의 책임이 아니다. 어드민의 재발행조차
 * 새 행을 만들지 않고 기존 행을 되돌린다.
 *
 * JPA 대신 JdbcClient 로 SQL 을 직접 쓴다. SKIP LOCKED 로 인스턴스 간 경합을 처리하고
 * 부분 인덱스를 정확히 타는 것이 이 서버의 핵심이라, 생성되는 SQL 을 통제해야 한다.
 */
@Repository
class OutboxRepository(
	private val jdbc: JdbcClient,
	private val properties: RelayProperties,
	private val backoffPolicy: BackoffPolicy,
) {

	/**
	 * 발행할 차례가 된 행을 최대 [limit] 건 집어 "내가 맡는다"고 표시하고 그 내용을 돌려준다.
	 * 상태가 PENDING 이고 next_attempt_at 이 지난 행이 대상이며, 집은 행은 PUBLISHING 이 된다.
	 *
	 * 고르는 것과 표시하는 것을 한 문장으로 묶었다. 나누면 그 사이에 다른 인스턴스가 같은 행을
	 * 집을 수 있고, 반대로 락을 쥔 채 Kafka 로 나가는 구조가 되기도 쉽다. 한 문장이면 그 틈이
	 * 아예 생기지 않고, 이 트랜잭션 안에서는 네트워크를 기다리는 구간이 없다.
	 *
	 * 정렬 순서를 idx_outbox_pending 인덱스의 컬럼 순서와 똑같이 맞춰 뒀다. 어긋나면 매 사이클
	 * 정렬 단계가 추가로 붙는다.
	 */
	fun claimBatch(limit: Int): List<OutboxEventRow> =
		jdbc.sql(CLAIM_SQL)
			.param("limit", limit)
			.param("instanceId", properties.instanceId)
			.query(ROW_MAPPER)
			.list()

	/**
	 * Kafka 가 받았다고 응답한 행들을 한 번의 UPDATE 로 PUBLISHED 로 내린다.
	 *
	 * WHERE 절이 status·locked_by·locked_at 세 조건을 모두 건다. 좀비 회수가 있는 한, 이
	 * 행을 선점했던 사이클이 아직 살아있다는 보장이 없다 — 발행 중에 락이 만료되어 다른
	 * 사이클(같은 인스턴스일 수도, 다른 인스턴스일 수도 있다)이 이미 다시 잡아 처리했을 수
	 * 있으므로, 결과를 쓰기 직전에 "지금도 내가 주인인가"를 다시 확인해야 한다.
	 *
	 * instanceId 만으로는 부족하다 — 릴레이가 한 대뿐이면 자기 자신이 회수당했다가 같은
	 * 사이클에서 재선점해도 instanceId 는 항상 같기 때문이다. claimedAt(=locked_at)이 그
	 * 빈틈을 메운다. 선점 SQL은 한 문장짜리 UPDATE라 그 안에서 now()가 한 번만 평가되므로,
	 * 같은 배치로 잡힌 행은 모두 같은 locked_at을 갖는다 — 즉 "이번 선점 시각"이 사이클 하나를
	 * 가리키는 값이 되고, 그래서 새 컬럼 없이도 배치 UPDATE를 유지한 채 소유권을 확인할 수 있다.
	 */
	fun markPublished(ids: List<UUID>, instanceId: String, claimedAt: java.time.Instant): Int {
		if (ids.isEmpty()) return 0
		return jdbc.sql(MARK_PUBLISHED_SQL)
			.param("ids", ids)
			.param("instanceId", instanceId)
			.param("claimedAt", java.sql.Timestamp.from(claimedAt))
			.update()
	}

	/**
	 * 발행에 실패한 행들을 기록한다. 재시도 횟수를 올리고, 한도에 도달했으면 DEAD 로,
	 * 아니면 PENDING 으로 되돌리면서 다음 시도 시각을 찍는다.
	 *
	 * 같은 에러 메시지를 공유하는 행들을 한 번에 받는다. Kafka 가 통째로 죽으면 배치 전체가
	 * 같은 메시지라 UPDATE 한 번으로 끝난다.
	 *
	 * 다음 시도 시각을 애플리케이션이 아니라 SQL 식으로 계산하는 이유는, 행마다 재시도 횟수가
	 * 달라서다. 애플리케이션에서 계산한 값 하나를 넘기면 모든 행이 같은 대기 시간을 갖게 되고,
	 * 제대로 하려면 행마다 UPDATE 를 따로 날려야 한다.
	 *
	 * [markPublished] 와 같은 소유권 조건을 결과 실패 기록에도 적용한다.
	 */
	fun markFailed(ids: List<UUID>, message: String, instanceId: String, claimedAt: java.time.Instant): Int {
		if (ids.isEmpty()) return 0
		return jdbc.sql(MARK_FAILED_SQL)
			.param("ids", ids)
			.param("message", message)
			.param("maxAttempts", properties.backoff.maxAttempts)
			.param("baseSeconds", backoffPolicy.baseSeconds)
			.param("maxSeconds", backoffPolicy.maxSeconds)
			.param("deadRecoveryDelaySeconds", properties.dead.recoveryDelay.toMillis() / 1000.0)
			.param("instanceId", instanceId)
			.param("claimedAt", java.sql.Timestamp.from(claimedAt))
			.update()
	}

	/**
	 * 영구 실패를 재시도 없이 곧바로 "정지된 DEAD"로 보낸다. [markFailed] 와 같은
	 * 소유권 조건을 쓰되, 시도 횟수 임계값을 보지 않고 무조건 DEAD + next_attempt_at = 'infinity'
	 * 다 — 새 상태나 컬럼을 만들지 않고 기존 정지 스위치를 재사용한다.
	 */
	fun markDead(ids: List<UUID>, message: String, instanceId: String, claimedAt: java.time.Instant): Int {
		if (ids.isEmpty()) return 0
		return jdbc.sql(MARK_DEAD_SQL)
			.param("ids", ids)
			.param("message", message)
			.param("instanceId", instanceId)
			.param("claimedAt", java.sql.Timestamp.from(claimedAt))
			.update()
	}

	/**
	 * PUBLISHING 으로 표시만 되고 끝나지 않은 행을 PENDING 으로 되돌린다. 발행 도중에 릴레이가
	 * 죽으면 그 행이 여기 걸린다. 표시한 지 정해진 시간이 지난 것을 기준으로 삼는다.
	 *
	 * 되돌리면서 재시도 횟수를 올린다. 올리지 않으면, 릴레이를 반복해서 죽게 만드는 행이
	 * 되돌리기와 재시도를 무한히 오가며 영원히 끝나지 않는다.
	 *
	 * 횟수가 한도를 넘었더라도 여기서는 DEAD 로 내리지 않고 항상 PENDING 으로만 보낸다.
	 * 이 시점에 아는 것은 "표시한 지 오래됐다" 뿐이고, Kafka 가 여전히 죽어 있다는 보장이 없다.
	 * 배포로 재시작했거나 잠깐 멈췄던 경우도 똑같이 여기 걸리는데, 그걸 DEAD 로 내리면
	 * 멀쩡한 이벤트가 복구 대기 시간만큼 발이 묶인다. DEAD 판정은 실제로 발행이 실패한
	 * 시점에서만 한다.
	 */
	fun reclaimZombies(): Int =
		jdbc.sql(RECLAIM_ZOMBIES_SQL)
			.param("lockTimeoutSeconds", properties.zombie.lockTimeout.toMillis() / 1000.0)
			.param("baseSeconds", backoffPolicy.baseSeconds)
			.param("maxSeconds", backoffPolicy.maxSeconds)
			.update()

	/**
	 * DEAD 를 종착역으로 두지 않고, 복구 대기 시간이 지난 행을 PENDING 으로 되살린다.
	 * 재시도 횟수도 0으로 되돌려 처음부터 다시 시작하게 한다.
	 *
	 * Kafka 가 오래 죽어 있었던 것은 이벤트 잘못이 아니므로, 사람 손을 빌리지 않고 회복되는
	 * 쪽이 맞다. 새 행을 만들지 않고 같은 행을 그대로 쓴다.
	 *
	 * next_attempt_at 이 'infinity' 인 행은 조건에서 자연스럽게 빠진다. 사람이 멈춰 둔
	 * 행이 그렇고, 이것이 이 자동 복구를 멈추는 유일한 수단이다.
	 */
	fun recoverDead(): Int =
		jdbc.sql(RECOVER_DEAD_SQL).update()

	/**
	 * 자동 복구를 타고 끝없이 도는 행을 사람이 멈춘다. next_attempt_at 을 'infinity' 로 밀면
	 * [recoverDead] 의 조건에서 빠지므로, 컬럼이나 상태를 새로 만들지 않고 정지가 된다.
	 *
	 * DEAD 인 행에만 적용한다. 상태를 확인하지 않으면 아직 발행을 기다리는 PENDING 행까지
	 * 멈출 수 있는데, 그렇게 멈춘 행은 정지 건수 집계에도 [findDead] 목록에도 안 잡히면서
	 * 영원히 발행되지 않는다. 정지 건수를 따로 세는 이유가 바로 "멈춰 놓고 잊는 것"을 막기
	 * 위해서인데, 그 감시망 밖으로 새는 셈이 된다.
	 */
	fun hold(id: UUID): Int =
		jdbc.sql(HOLD_SQL).param("id", id).update()

	/** 정지를 푼다. 다음 자동 복구에서 다시 잡힌다. [hold] 와 같은 이유로 DEAD 행에만 적용한다. */
	fun release(id: UUID): Int =
		jdbc.sql(RELEASE_SQL).param("id", id).update()

	/**
	 * 자동 복구를 기다리지 않고 지금 바로 다시 발행하게 한다. 기존 행을 되돌릴 뿐
	 * 새 행을 만들지 않는다. [hold] 와 같은 이유로 DEAD 행에만 적용한다.
	 */
	fun republish(id: UUID): Int =
		jdbc.sql(REPUBLISH_SQL).param("id", id).update()

	/**
	 * 자동 복구를 기다리지 않고 지금 바로 다시 발행하게 한다. [republish] 와 달리 대상이
	 * PUBLISHED 행이다 — 브로커 재시작으로 PUBLISHED 기록과 달리 실제로는 유실된 메시지를
	 * 되돌리거나, 워커가 "처리되지 않았다"고 판정해 재발행을 요청할 때 쓴다.
	 */
	fun forceRepublish(id: UUID): Int =
		jdbc.sql(FORCE_REPUBLISH_SQL).param("id", id).update()

	/**
	 * 상태별 건수를 센다. 지표 갱신과 어드민 조회가 같이 쓴다.
	 *
	 * 한 번에 세지 않고 네 개의 서브쿼리로 나눈 이유는, PENDING 과 PUBLISHING 의 조건을
	 * 각각의 부분 인덱스 조건과 글자 그대로 일치시키기 위해서다. 일치해야 옵티마이저가
	 * 테이블을 읽지 않고 인덱스만 훑는다.
	 *
	 * DEAD 와 정지 건수는 맞는 부분 인덱스가 없어 테이블 전체를 읽는다. DEAD 행이 적다는
	 * 전제로 감수하고 있고, PUBLISHED 행이 계속 쌓이면 그만큼 비싸진다.
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

	/**
	 * 어드민이 보는 DEAD 목록. 사람이 멈춰 둔 행도 함께 나오되 [DeadEventView.held] 로 구분한다.
	 * 정지된 행은 상태가 여전히 DEAD 라서 이 플래그 없이는 멈춰 있다는 것이 보이지 않는다.
	 */
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
				// getLong 뒤에 wasNull 을 반드시 확인한다. getLong 은 SQL NULL 에 예외를 내지 않고
				// 0 을 돌려주므로, 이 확인을 빼면 DOCUMENT_DELETED 처럼 값이 없는 이벤트가 0 번
				// 버전으로 둔갑해 오류 없이 그대로 발행된다.
				documentVersionId = rs.getLong("document_version_id").takeUnless { rs.wasNull() },
				eventType = rs.getString("event_type"),
				eventSchemaVersion = rs.getInt("event_schema_version"),
				payload = rs.getString("payload"),
				traceId = rs.getString("trace_id"),
				publishAttemptCount = rs.getInt("publish_attempt_count"),
				createdAt = rs.getTimestamp("created_at").toInstant(),
				lockedAt = rs.getTimestamp("locked_at").toInstant(),
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
			          o.trace_id, o.publish_attempt_count, o.created_at, o.locked_at
			""".trimIndent()

		// 여러 id 를 넘길 때 = ANY(:ids) 가 아니라 IN (:ids) 를 쓴다. PostgreSQL JDBC 드라이버가
		// UUID 목록을 배열 타입으로 자동 변환해 주지 않아서, = ANY 를 쓰면 오류 없이 조용히
		// 한 건도 매칭되지 않을 수 있다. IN 에 목록을 바인딩하면 스프링이 자리표시자를
		// 개수만큼 펼쳐 주므로 그 문제가 없다.
		val MARK_PUBLISHED_SQL = """
			UPDATE outbox_event
			   SET status = 'PUBLISHED',
			       published_at = now(),
			       locked_by = NULL,
			       locked_at = NULL
			 WHERE id IN (:ids)
			   AND status = 'PUBLISHING'
			   AND locked_by = :instanceId
			   AND locked_at = :claimedAt
			""".trimIndent()

		val MARK_FAILED_SQL = """
			UPDATE outbox_event
			   SET publish_attempt_count = publish_attempt_count + 1,
			       status = CASE
			           WHEN publish_attempt_count + 1 >= :maxAttempts THEN 'DEAD'
			           ELSE 'PENDING'
			       END,
			       next_attempt_at = now() + CASE
			           WHEN publish_attempt_count + 1 >= :maxAttempts THEN :deadRecoveryDelaySeconds
			           ELSE ${BackoffPolicy.sqlWith("publish_attempt_count")}
			       END * INTERVAL '1 second',
			       last_error_message = :message,
			       locked_by = NULL,
			       locked_at = NULL
			 WHERE id IN (:ids)
			   AND status = 'PUBLISHING'
			   AND locked_by = :instanceId
			   AND locked_at = :claimedAt
			""".trimIndent()

		val MARK_DEAD_SQL = """
			UPDATE outbox_event
			   SET status = 'DEAD',
			       publish_attempt_count = publish_attempt_count + 1,
			       next_attempt_at = 'infinity',
			       last_error_message = :message,
			       locked_by = NULL,
			       locked_at = NULL
			 WHERE id IN (:ids)
			   AND status = 'PUBLISHING'
			   AND locked_by = :instanceId
			   AND locked_at = :claimedAt
			""".trimIndent()

		val RECLAIM_ZOMBIES_SQL = """
			UPDATE outbox_event
			   SET status = 'PENDING',
			       publish_attempt_count = publish_attempt_count + 1,
			       next_attempt_at = now() +
			           ${BackoffPolicy.sqlWith("publish_attempt_count")}
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

		val FORCE_REPUBLISH_SQL = """
			UPDATE outbox_event
			   SET status = 'PENDING', publish_attempt_count = 0, next_attempt_at = now(),
			       locked_by = NULL, locked_at = NULL
			 WHERE id = :id AND status = 'PUBLISHED'
			""".trimIndent()
	}
}
