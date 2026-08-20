package aidocs.doc_relay

import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.jdbc.datasource.SingleConnectionDataSource
import org.testcontainers.postgresql.PostgreSQLContainer
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 컨테이너를 테스트마다 직접 띄운다. 스프링 컨텍스트를 쓰면 깨진 스키마에서
 * 컨텍스트 자체가 안 떠서 무엇이 실패했는지 확인할 수 없다.
 */
class SchemaValidatorTest {

	private fun validatorOn(initScript: String): Pair<SchemaValidator, PostgreSQLContainer> {
		val container = PostgreSQLContainer("postgres:17-alpine").withInitScript(initScript)
		container.start()
		val dataSource = SingleConnectionDataSource(
			container.jdbcUrl, container.username, container.password, true,
		)
		return SchemaValidator(JdbcClient.create(dataSource)) to container
	}

	@Test
	fun `passes on the agreed schema`() {
		val (validator, container) = validatorOn("schema/outbox.sql")
		container.use { validator.afterPropertiesSet() }
	}

	@Test
	fun `fails fast when next_attempt_at is missing or nullable`() {
		val (validator, container) = validatorOn("schema/outbox-broken.sql")
		container.use {
			val error = assertFailsWith<IllegalStateException> { validator.afterPropertiesSet() }
			assertTrue(
				error.message!!.contains("next_attempt_at"),
				"어떤 컬럼이 문제인지 메시지에 있어야 한다: ${error.message}",
			)
		}
	}

	@Test
	fun `fails fast when next_attempt_at exists but is nullable`() {
		val (validator, container) = validatorOn("schema/outbox-nullable.sql")
		container.use {
			val error = assertFailsWith<IllegalStateException> { validator.afterPropertiesSet() }
			assertTrue(
				error.message!!.contains("next_attempt_at"),
				"어떤 컬럼이 문제인지 메시지에 있어야 한다: ${error.message}",
			)
		}
	}

	@Test
	fun `fails fast when the status check does not allow DEAD`() {
		val (validator, container) = validatorOn("schema/outbox-broken.sql")
		container.use {
			val error = assertFailsWith<IllegalStateException> { validator.afterPropertiesSet() }
			assertTrue(error.message!!.contains("DEAD"), "메시지: ${error.message}")
		}
	}

	@Test
	fun `only warns when an index is missing`() {
		val (validator, container) = validatorOn("schema/outbox-no-index.sql")
		// 인덱스는 없어도 결과가 맞으므로 기동을 막지 않는다. 예외가 안 나면 통과다.
		container.use { validator.afterPropertiesSet() }
	}
}
