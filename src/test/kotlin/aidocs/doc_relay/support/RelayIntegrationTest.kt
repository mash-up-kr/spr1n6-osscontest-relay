package aidocs.doc_relay.support

import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.test.context.TestPropertySource
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.Instant
import java.util.UUID

@SpringBootTest
@Testcontainers
@TestPropertySource(
	properties = [
		"relay.polling.interval=1h",
		"relay.zombie.scan-interval=1h",
		"relay.dead.recovery-scan-interval=1h",
		"relay.metrics.gauge-refresh-interval=1h",
		"relay.listener.enabled=false",
	]
)
abstract class RelayIntegrationTest {

	// 컨테이너를 여러 테스트 클래스가 공유하는 싱글턴으로 띄운다.
	// @Container 를 상속된 정적 필드에 붙이면 JUnit5 확장이 테스트 클래스마다 start/stop 을 반복해
	// (테스트 클래스 A 종료 시 stop → 클래스 B 시작 시 재기동으로 포트가 바뀌고, Spring 이 캐싱한
	// ApplicationContext/HikariPool 은 죽은 포트를 계속 참조하게 된다) 두 번째 클래스부터 커넥션이 끊긴다.
	// 그래서 @Container 를 쓰지 않고 companion object 초기화 시점에 직접 start() 해
	// JVM 전체에서 한 번만 뜨고 Ryuk 이 종료 시 정리하도록 한다 (Testcontainers 공식 "싱글턴 컨테이너" 패턴).
	companion object {
		@ServiceConnection
		@JvmStatic
		val postgres: PostgreSQLContainer = PostgreSQLContainer("postgres:17-alpine")
			.withInitScript("schema/outbox.sql")
			.also { it.start() }

		@ServiceConnection
		@JvmStatic
		val kafka: KafkaContainer = KafkaContainer("apache/kafka:4.0.0")
			.also { it.start() }
	}

	@Autowired
	protected lateinit var jdbc: JdbcClient

	@BeforeEach
	fun resetFixture() {
		jdbc.sql("TRUNCATE outbox_event, document_version, document, tenant RESTART IDENTITY CASCADE").update()
	}

	/** tenant + document 를 만들고 documentId 를 돌려준다. */
	protected fun seedParents(): Long {
		val tenantId = jdbc.sql("INSERT INTO tenant (name) VALUES ('acme') RETURNING id")
			.query(Long::class.java).single()
		return jdbc.sql(
			"""
			INSERT INTO document (tenant_id, owner_principal_id, title)
			VALUES (:tenantId, 'USER:1', 'contract')
			RETURNING id
			""".trimIndent()
		).param("tenantId", tenantId).query(Long::class.java).single()
	}

	/** document_version 을 INSERT 해 트리거가 outbox 행을 만들게 한다. */
	protected fun insertVersion(documentId: Long, versionNo: Long = 1): Long =
		jdbc.sql(
			"""
			INSERT INTO document_version (
				document_id, version_no, source_object_key, original_filename,
				mime_type, file_size, content_hash, created_by_principal_id
			) VALUES (
				:documentId, :versionNo, 'tenant-1/doc/v1/contract.pdf', '\x00'::bytea,
				'application/pdf', 1048576, 'sha256:abc', 'USER:1'
			) RETURNING id
			""".trimIndent()
		).param("documentId", documentId).param("versionNo", versionNo)
			.query(Long::class.java).single()

	/** outbox 행을 원하는 상태로 직접 만든다. 테스트 준비 전용이며 운영 코드는 INSERT 하지 않는다. */
	protected fun insertOutbox(
		documentId: Long,
		/** DOCUMENT_DELETED 는 문서 단위 이벤트라 null 이다. 스키마의 CHECK 가 이 관계를 강제한다. */
		documentVersionId: Long?,
		eventType: String = "INDEXING_REQUESTED",
		status: String = "PENDING",
		attemptCount: Int = 0,
		// JVM 시각과 Postgres now() 사이 clock skew 때문에 "정확히 지금"을 기본값으로 두면
		// 선점 쿼리가 산발적으로 행을 놓친다. 기본값의 의미는 "확실히 만기가 지난 행"이다.
		nextAttemptAt: Instant = Instant.now().minusSeconds(60),
		lockedAt: Instant? = null,
	): UUID {
		val id = UUID.randomUUID()
		jdbc.sql(
			"""
			INSERT INTO outbox_event (
				id, tenant_id, document_id, document_version_id, event_type,
				payload, trace_id, status, publish_attempt_count, next_attempt_at,
				locked_by, locked_at
			)
			SELECT :id, d.tenant_id, :documentId, :versionId, :eventType,
			       '{"versionNo":1,"occurredAt":"2026-08-13T09:14:22Z"}'::jsonb, 'trace-1',
			       :status, :attemptCount, :nextAttemptAt,
			       CASE WHEN :lockedAt::timestamptz IS NULL THEN NULL ELSE 'other-instance' END, :lockedAt
			  FROM document d WHERE d.id = :documentId
			""".trimIndent()
		)
			.param("id", id)
			.param("documentId", documentId)
			.param("versionId", documentVersionId)
			.param("eventType", eventType)
			.param("status", status)
			.param("attemptCount", attemptCount)
			.param("nextAttemptAt", nextAttemptAt.let { java.sql.Timestamp.from(it) })
			.param("lockedAt", lockedAt?.let { java.sql.Timestamp.from(it) })
			.update()
		return id
	}

	protected fun statusOf(id: UUID): String =
		jdbc.sql("SELECT status FROM outbox_event WHERE id = :id")
			.param("id", id).query(String::class.java).single()
}
