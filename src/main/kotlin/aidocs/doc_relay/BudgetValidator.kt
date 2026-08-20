package aidocs.doc_relay

import org.springframework.beans.factory.InitializingBean
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * 기동 시 "한 사이클의 최대 소요 시간 < 좀비 회수 타임아웃"을 검증한다.
 *
 * 이 관계가 깨지면 좀비 회수가 아직 발행 중인 배치를 뺏어 간다 — 결과를 쓸 때 소유권을
 * 확인하는 로직이 막으려는 경합이 실제로 벌어진다. 설정을 잘못 바꾸면 조용히 깨지므로,
 * 스키마 검증과 같은 방식으로 기동 자체를 막는다.
 *
 * DB 왕복 여유(10초)는 Hikari(Spring Boot 기본 커넥션 풀) 타임아웃(connection-timeout=5000ms)이
 * 실제로 보장하는 값에 안전 마진을 얹은 고정값이라 프로퍼티로 빼지 않는다. 이 여유를 30초로
 * 두면 demo 프로파일의 zombie.lock-timeout(30초)과 산술적으로 절대 양립할 수 없다 — producer
 * 타임아웃을 0에 가깝게 줄여도 0+0+30 은 이미 30 과 같아서 `<` 조건을 만족 못한다. 10초는
 * 운영(140s < 300s)과 demo(23s < 30s) 양쪽 모두를 실제로 만족시키면서, 여전히 5초 커넥션
 * 타임아웃보다 넉넉한 마진이다.
 */
@Component
class BudgetValidator(private val properties: RelayProperties) : InitializingBean {

	override fun afterPropertiesSet() {
		val cycleUpperBound = properties.kafka.producer.maxBlock
			.plus(properties.kafka.producer.deliveryTimeout)
			.plus(DB_ROUND_TRIP_ALLOWANCE)
		val zombieTimeout = properties.zombie.lockTimeout

		check(cycleUpperBound < zombieTimeout) {
			"한 사이클 상한(${cycleUpperBound})이 좀비 회수 타임아웃(${zombieTimeout}) 이상이다. " +
				"회수가 아직 발행 중인 배치를 가로챌 수 있다. " +
				"kafka.producer.max-block(${properties.kafka.producer.maxBlock} / ${properties.kafka.producer.maxBlock.toSeconds()}s)/" +
				"delivery-timeout(${properties.kafka.producer.deliveryTimeout} / ${properties.kafka.producer.deliveryTimeout.toSeconds()}s)을 줄이거나 " +
				"zombie.lock-timeout 을 늘려라."
		}
	}

	private companion object {
		val DB_ROUND_TRIP_ALLOWANCE: Duration = Duration.ofSeconds(10)
	}
}
