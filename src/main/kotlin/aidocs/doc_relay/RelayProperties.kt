package aidocs.doc_relay

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

/**
 * spec §9. 모든 타임아웃·주기·상한은 여기로만 들어온다.
 * 코드에 숫자를 하드코딩하지 않는다.
 */
@ConfigurationProperties(prefix = "relay")
data class RelayProperties(
	/** locked_by 에 기록되는 인스턴스 식별자. */
	val instanceId: String = "doc-relay-local",
	val drain: Drain = Drain(),
	val polling: Polling = Polling(),
	val backoff: Backoff = Backoff(),
	val dead: Dead = Dead(),
	val zombie: Zombie = Zombie(),
	val listener: Listener = Listener(),
	val metrics: Metrics = Metrics(),
	val kafka: Kafka = Kafka(),
) {
	data class Drain(val batchSize: Int = 100)

	data class Polling(val interval: Duration = Duration.ofSeconds(10))

	data class Backoff(
		val base: Duration = Duration.ofSeconds(10),
		val max: Duration = Duration.ofMinutes(5),
		/** publish_attempt_count 가 이 값에 도달하면 DEAD. */
		val maxAttempts: Int = 5,
	)

	data class Dead(
		/** DEAD 전환 시 next_attempt_at 에 더하는 오프셋. 복구 스캐너의 기준이 된다. */
		val recoveryDelay: Duration = Duration.ofMinutes(10),
		val recoveryScanInterval: Duration = Duration.ofMinutes(5),
	)

	data class Zombie(
		val lockTimeout: Duration = Duration.ofMinutes(5),
		val scanInterval: Duration = Duration.ofMinutes(1),
	)

	data class Listener(
		/** 테스트에서 리스너를 꺼 백그라운드 드레인이 단언과 경합하지 않게 한다. */
		val enabled: Boolean = true,
		val channel: String = "outbox_event",
		val reconnectBase: Duration = Duration.ofSeconds(1),
		val reconnectMax: Duration = Duration.ofSeconds(30),
	)

	data class Metrics(val gaugeRefreshInterval: Duration = Duration.ofSeconds(30))

	data class Kafka(
		val topic: String = "doc.events.v1",
		val partitions: Int = 3,
	)
}
