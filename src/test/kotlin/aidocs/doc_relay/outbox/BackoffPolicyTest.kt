package aidocs.doc_relay.outbox

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import kotlin.test.assertEquals

class BackoffPolicyTest : RelayIntegrationTest() {

	@Autowired
	private lateinit var policy: BackoffPolicy

	private fun delayAfter(attemptCount: Int): Duration {
		val expression = BackoffPolicy.sqlWith(attemptCount.toString())
		val seconds = jdbc.sql("SELECT EXTRACT(EPOCH FROM ($expression * INTERVAL '1 second'))")
			.param("baseSeconds", policy.baseSeconds)
			.param("maxSeconds", policy.maxSeconds)
			.query(Double::class.java).single()
		return Duration.ofMillis((seconds * 1000).toLong())
	}

	@Test
	fun `schedule matches the backoff table`() {
		// 1회 실패 후 10s, 2회 20s, 3회 40s, 4회 80s
		assertEquals(Duration.ofSeconds(10), delayAfter(0))
		assertEquals(Duration.ofSeconds(20), delayAfter(1))
		assertEquals(Duration.ofSeconds(40), delayAfter(2))
		assertEquals(Duration.ofSeconds(80), delayAfter(3))
	}

	@Test
	fun `delay is capped at max`() {
		// base 10s, max 5m -> 2^5=320s 부터 cap 이 걸린다
		assertEquals(Duration.ofMinutes(5), delayAfter(5))
		assertEquals(Duration.ofMinutes(5), delayAfter(20))
	}
}
