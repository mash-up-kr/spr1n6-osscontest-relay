package aidocs.doc_relay

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

/**
 * 릴레이가 쓰는 모든 시간값과 상한이 여기로 들어온다. 코드 어디에도 숫자를 직접 박지 않는다.
 * 여기 있는 기본값은 운영 기준이고, 시연용으로 훨씬 짧게 줄인 값은 demo 프로파일에 따로 둔다.
 */
@ConfigurationProperties(prefix = "relay")
data class RelayProperties(
	/**
	 * 행을 집을 때 locked_by 에 남기는 이름. 어느 인스턴스가 이 행을 맡았는지 추적하는 데 쓴다.
	 * 여러 대를 띄울 때 서로 구분되도록 호스트 이름을 넣는다.
	 */
	val instanceId: String = "doc-relay-local",
	val drain: Drain = Drain(),
	val polling: Polling = Polling(),
	val backoff: Backoff = Backoff(),
	val dead: Dead = Dead(),
	val zombie: Zombie = Zombie(),
	val listener: Listener = Listener(),
	val metrics: Metrics = Metrics(),
	val kafka: Kafka = Kafka(),
	val shutdown: Shutdown = Shutdown(),
	val admin: Admin = Admin(),
) {
	/** [batchSize] 는 한 사이클에 집어 올 행의 최대 개수. 이만큼 꽉 채워 오면 아직 밀린 게 있다는 뜻이다. */
	data class Drain(val batchSize: Int = 100)

	/** DB 알림이 유실되거나 리스너가 죽어도 이 주기마다 한 번씩은 확인한다. 최후의 안전망이다. */
	data class Polling(val interval: Duration = Duration.ofSeconds(10))

	/**
	 * 발행 실패 후 다시 시도하기까지의 대기 시간. [base] 에서 시작해 실패할 때마다 두 배가 되고
	 * [max] 를 넘지 않는다. 실패 횟수가 [maxAttempts] 에 닿으면 더 시도하지 않고 DEAD 로 보낸다.
	 */
	data class Backoff(
		val base: Duration = Duration.ofSeconds(10),
		val max: Duration = Duration.ofMinutes(5),
		val maxAttempts: Int = 5,
	)

	/**
	 * DEAD 는 종착역이 아니다. [recoveryDelay] 가 지나면 자동으로 되살아나고,
	 * [recoveryScanInterval] 마다 되살릴 행이 있는지 확인한다.
	 */
	data class Dead(
		val recoveryDelay: Duration = Duration.ofMinutes(10),
		val recoveryScanInterval: Duration = Duration.ofMinutes(5),
	)

	/**
	 * 집어만 놓고 끝나지 않은 행을 되돌리는 기준. 집은 지 [lockTimeout] 이 지나면 회수 대상이 되고,
	 * [scanInterval] 마다 확인한다.
	 *
	 * [lockTimeout] 은 발행 한 번에 걸릴 수 있는 최대 시간보다 넉넉히 길어야 한다. 짧으면 아직
	 * 발행 중인 배치를 회수가 가로채고, 같은 행을 두 사이클이 동시에 만지게 된다.
	 */
	data class Zombie(
		val lockTimeout: Duration = Duration.ofMinutes(5),
		val scanInterval: Duration = Duration.ofMinutes(1),
	)

	/**
	 * DB 알림 리스너. 끊기면 [reconnectBase] 부터 시작해 두 배씩 늘려 가며 [reconnectMax] 까지
	 * 기다렸다 다시 붙는다.
	 *
	 * [enabled] 는 테스트에서 리스너를 꺼 두기 위한 스위치다. 켜 두면 백그라운드 발행이 돌면서
	 * 테스트가 확인하려는 상태를 먼저 바꿔 버린다.
	 */
	data class Listener(
		val enabled: Boolean = true,
		val channel: String = "outbox_event",
		val reconnectBase: Duration = Duration.ofSeconds(1),
		val reconnectMax: Duration = Duration.ofSeconds(30),
	)

	/** 상태별 건수를 다시 세는 주기. 짧을수록 지표가 최신이지만 그만큼 DB 를 자주 훑는다. */
	data class Metrics(val gaugeRefreshInterval: Duration = Duration.ofSeconds(30))

	data class Kafka(
		val topic: String = "doc.events.v1",
		val partitions: Int = 3,
		val producer: Producer = Producer(),
	) {
		/**
		 * 기본값에 맡기지 않고 명시한다. 예산 관계(사이클 상한 < 좀비 회수 타임아웃)가
		 * 이 값들 위에 서 있어서, 기본값이 조용히 바뀌면 그 관계도 조용히 깨진다.
		 */
		data class Producer(
			val maxBlock: Duration = Duration.ofSeconds(10),
			val requestTimeout: Duration = Duration.ofSeconds(30),
			val deliveryTimeout: Duration = Duration.ofSeconds(120),
			val maxRequestSize: Int = 1_048_576,
		)
	}

	/** 정상 종료 시 진행 중인 드레인 사이클을 기다리는 상한. */
	data class Shutdown(val drainTimeout: Duration = Duration.ofSeconds(30))

	/** 쓰기 액션 최소 인증. [token] 이 비어 있으면(기본값) 인증은 비활성이다. */
	data class Admin(val token: String = "")
}
