package aidocs.doc_relay

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.InitializingBean
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Component

/**
 * 기동할 때 outbox_event 테이블이 릴레이가 기대하는 형태인지 확인한다. 이 테이블의 스키마는
 * API 서버 저장소가 소유하므로, 릴레이는 맞는지 확인만 하고 고치지 않는다.
 *
 * 컬럼과 제약이 어긋나면 기동을 중단한다. 특히 next_attempt_at 이 없거나 값이 비어 있을 수
 * 있으면, 발행할 행을 고르는 쿼리가 오류 없이 조용히 0건을 돌려준다. 릴레이는 "보낼 게 없다"고
 * 판단해 정상인 것처럼 굴고, 이벤트는 계속 쌓이는데 아무도 눈치채지 못한다. 그런 상태로 도는
 * 것보다 아예 안 뜨는 편이 낫다.
 *
 * 인덱스는 없어도 결과가 맞고 느려지기만 하므로 경고만 남긴다.
 */
@Component
class SchemaValidator(private val jdbc: JdbcClient) : InitializingBean {

	private val log = LoggerFactory.getLogger(javaClass)

	override fun afterPropertiesSet() {
		val problems = mutableListOf<String>()

		val nextAttemptAt = jdbc.sql(
			"""
			SELECT is_nullable
			  FROM information_schema.columns
			 WHERE table_name = 'outbox_event' AND column_name = 'next_attempt_at'
			""".trimIndent()
		).query(String::class.java).optional()

		if (nextAttemptAt.isEmpty) {
			problems += "outbox_event.next_attempt_at 컬럼이 없다. " +
				"파트너 마이그레이션이 next_retry_at 을 아직 리네이밍하지 않았을 수 있다."
		} else if (nextAttemptAt.get() == "YES") {
			problems += "outbox_event.next_attempt_at 이 nullable 이다. " +
				"NOT NULL DEFAULT CURRENT_TIMESTAMP 여야 선점 쿼리가 행을 집는다."
		}

		REQUIRED_COLUMNS.forEach { column ->
			val exists = jdbc.sql(
				"""
				SELECT count(*)
				  FROM information_schema.columns
				 WHERE table_name = 'outbox_event' AND column_name = :column
				""".trimIndent()
			).param("column", column).query(Int::class.java).single()
			if (exists == 0) problems += "outbox_event.$column 컬럼이 없다."
		}

		val statusCheck = jdbc.sql(
			"""
			SELECT pg_get_constraintdef(oid)
			  FROM pg_constraint
			 WHERE conname = 'ck_outbox_status'
			""".trimIndent()
		).query(String::class.java).optional()

		if (statusCheck.isEmpty) {
			problems += "ck_outbox_status 제약이 없다."
		} else if (!statusCheck.get().contains("DEAD")) {
			problems += "ck_outbox_status 가 'DEAD' 를 허용하지 않는다. " +
				"현재 정의: ${statusCheck.get()}"
		}

		check(problems.isEmpty()) {
			"outbox_event 스키마가 릴레이가 기대하는 형태가 아니다:\n" +
				problems.joinToString("\n") { "  - $it" }
		}

		REQUIRED_INDEXES.forEach { index ->
			val exists = jdbc.sql("SELECT count(*) FROM pg_indexes WHERE indexname = :name")
				.param("name", index).query(Int::class.java).single()
			if (exists == 0) {
				log.warn("인덱스 {} 가 없다. 동작은 하지만 선점/회수 쿼리가 느려진다.", index)
			}
		}
	}

	private companion object {
		val REQUIRED_COLUMNS = listOf(
			"id", "tenant_id", "document_id", "document_version_id",
			"event_type", "event_schema_version", "payload", "trace_id",
			"status", "publish_attempt_count", "locked_by", "locked_at",
			"published_at", "last_error_message", "created_at",
		)
		val REQUIRED_INDEXES = listOf("idx_outbox_pending", "idx_outbox_stuck")
	}
}
