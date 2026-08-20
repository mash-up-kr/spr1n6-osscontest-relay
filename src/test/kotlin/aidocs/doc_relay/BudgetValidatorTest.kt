package aidocs.doc_relay

import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class BudgetValidatorTest {

	@Test
	fun `fails fast when the cycle upper bound reaches the zombie timeout`() {
		// cycleUpperBound = maxBlock 60s + deliveryTimeout 120s + DB 여유 10s = 190s, 좀비
		// 타임아웃 10s 보다 커서 검증에 실패해야 한다.
		val properties = RelayProperties(
			zombie = RelayProperties.Zombie(lockTimeout = Duration.ofSeconds(10)),
			kafka = RelayProperties.Kafka(
				producer = RelayProperties.Kafka.Producer(
					maxBlock = Duration.ofSeconds(60),
					deliveryTimeout = Duration.ofSeconds(120),
				)
			),
		)

		val error = assertFailsWith<IllegalStateException> { BudgetValidator(properties).afterPropertiesSet() }

		assertTrue(error.message!!.contains("PT10S") || error.message!!.contains("10"), "좀비 타임아웃 값이 메시지에 있어야 한다: ${error.message}")
		assertTrue(error.message!!.contains("60") && error.message!!.contains("120"), "프로듀서 상한 값이 메시지에 있어야 한다: ${error.message}")
	}

	@Test
	fun `passes with production defaults`() {
		// Kafka 130s(maxBlock 10s + deliveryTimeout 120s) + DB 여유 10s = 140s < 좀비 300s.
		BudgetValidator(RelayProperties()).afterPropertiesSet()
	}

	@Test
	fun `passes with demo profile values`() {
		// demo 프로파일: maxBlock 3s + deliveryTimeout 10s + DB 여유 10s = 23s < 좀비 30s.
		val properties = RelayProperties(
			zombie = RelayProperties.Zombie(lockTimeout = Duration.ofSeconds(30)),
			kafka = RelayProperties.Kafka(
				producer = RelayProperties.Kafka.Producer(
					maxBlock = Duration.ofSeconds(3),
					deliveryTimeout = Duration.ofSeconds(10),
				)
			),
		)

		BudgetValidator(properties).afterPropertiesSet()
	}
}
