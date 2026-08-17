package aidocs.doc_relay.outbox

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import kotlin.test.assertEquals

class BackoffPolicyTest : RelayIntegrationTest() {

	@Autowired
	private lateinit var policy: BackoffPolicy

	@Test
	fun `schedule matches the spec table`() {
		// spec §6 백오프 표: 1회 실패 후 10s, 2회 20s, 3회 40s, 4회 80s
		assertEquals(Duration.ofSeconds(10), policy.delayAfter(0))
		assertEquals(Duration.ofSeconds(20), policy.delayAfter(1))
		assertEquals(Duration.ofSeconds(40), policy.delayAfter(2))
		assertEquals(Duration.ofSeconds(80), policy.delayAfter(3))
	}

	@Test
	fun `delay is capped at max`() {
		// base 10s, max 5m -> 2^5=320s 부터 cap 이 걸린다
		assertEquals(Duration.ofMinutes(5), policy.delayAfter(5))
		assertEquals(Duration.ofMinutes(5), policy.delayAfter(20))
	}

	@Test
	fun `sql expression produces the same seconds as kotlin`() {
		// SQL 식과 Kotlin 구현이 어긋나면 재시도 간격이 조용히 달라진다. 여기서 고정한다.
		(0..9).forEach { n ->
			val fromSql = jdbc.sql(
				"SELECT EXTRACT(EPOCH FROM (${BackoffPolicy.SQL_EXPRESSION} * INTERVAL '1 second'))"
					.replace(":attemptCount", n.toString())
			)
				.param("baseSeconds", policy.baseSeconds)
				.param("maxSeconds", policy.maxSeconds)
				.query(Double::class.java).single()

			assertEquals(
				policy.delayAfter(n).toMillis() / 1000.0,
				fromSql,
				0.001,
				"n=$n 에서 SQL 과 Kotlin 백오프가 다르다",
			)
		}
	}
}
