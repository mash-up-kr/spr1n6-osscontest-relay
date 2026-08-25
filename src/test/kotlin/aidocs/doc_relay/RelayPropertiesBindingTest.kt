package aidocs.doc_relay

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import kotlin.test.assertEquals

/**
 * application.yaml 의 키가 실제로 RelayProperties 에 바인딩되는지 확인한다.
 *
 * 기본값만 보는 테스트로는 YAML 오타를 잡지 못한다. Kotlin 기본값과 YAML 값이 같아서,
 * `bacth-size` 처럼 잘못 적은 키는 오류 없이 조용히 같은 기본값으로 떨어지기 때문이다.
 * 그래서 스프링 컨텍스트가 실제로 만든 빈의 값을 단언한다.
 *
 * RelayIntegrationTest 의 @TestPropertySource 가 덮어쓰는 다섯 개는 제외하고, 덮어쓰지 않은
 * 값만 단언한다 — 그 값들만이 YAML 바인딩 경로를 지난 값이다.
 */
class RelayPropertiesBindingTest : RelayIntegrationTest() {

	@Autowired
	private lateinit var properties: RelayProperties

	@Test
	fun `application yaml binds to relay properties`() {
		// 드레인 배치 크기
		assertEquals(100, properties.drain.batchSize)

		// 백오프
		assertEquals(Duration.ofSeconds(10), properties.backoff.base)
		assertEquals(Duration.ofMinutes(5), properties.backoff.max)
		assertEquals(5, properties.backoff.maxAttempts)

		// DEAD 복구 지연 (스캔 주기만 덮어쓴다)
		assertEquals(Duration.ofMinutes(10), properties.dead.recoveryDelay)

		// 좀비 락 타임아웃 (스캔 주기만 덮어쓴다)
		assertEquals(Duration.ofMinutes(5), properties.zombie.lockTimeout)

		// 리스너 채널과 재연결 설정 (enabled 만 덮어쓴다)
		assertEquals("outbox_event", properties.listener.channel)
		assertEquals(Duration.ofSeconds(1), properties.listener.reconnectBase)
		assertEquals(Duration.ofSeconds(30), properties.listener.reconnectMax)

		// Kafka 토픽과 파티션
		assertEquals("doc.events.v1", properties.kafka.topic)
		assertEquals(3, properties.kafka.partitions)

		// Kafka 프로듀서 타임아웃과 메시지 상한
		assertEquals(Duration.ofSeconds(10), properties.kafka.producer.maxBlock)
		assertEquals(Duration.ofSeconds(30), properties.kafka.producer.requestTimeout)
		assertEquals(Duration.ofSeconds(120), properties.kafka.producer.deliveryTimeout)
		assertEquals(1_048_576, properties.kafka.producer.maxRequestSize)

		// 정상 종료 드레인 대기 상한
		assertEquals(Duration.ofSeconds(30), properties.shutdown.drainTimeout)
	}
}
