package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

/**
 * Kafka 로 보내는 쪽 설정.
 *
 * 모든 복제본이 받았을 때만 성공으로 치고(`acks=all`), 재시도로 같은 메시지가 여러 번
 * 들어가지 않도록 멱등 프로듀서를 켠다. Kafka 트랜잭션은 쓰지 않는다. DB 트랜잭션과 묶을
 * 수 있는 것도 아니어서 얻는 것에 비해 비용이 크다.
 *
 * 메시지 값을 바이트 그대로 다룬다. 본문 JSON 은 [EnvelopeAssembler] 가 이미 완성해 두었고,
 * 여기서 다시 직렬화를 태우면 payload 를 손대지 않고 그대로 옮긴다는 전제가 깨질 수 있다.
 */
@Configuration
class KafkaProducerConfig(
	private val kafkaProperties: KafkaProperties,
	private val connectionDetails: KafkaConnectionDetails,
	private val relayProperties: RelayProperties,
) {

	@Bean
	fun relayProducerFactory(): ProducerFactory<String, ByteArray> {
		val configs = kafkaProperties.buildProducerProperties().toMutableMap()
		// 접속 주소는 설정 파일이 아니라 별도로 주입받은 값으로 덮어쓴다. 테스트에서는
		// Testcontainers 가 띄운 브로커 주소가 이 경로로 들어오는데, 설정 파일만 읽으면
		// 컨테이너가 아니라 기본 주소를 가리키게 된다.
		configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = connectionDetails.bootstrapServers
		configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
		configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
		configs[ProducerConfig.ACKS_CONFIG] = "all"
		configs[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = true
		return DefaultKafkaProducerFactory(configs)
	}

	@Bean
	fun relayKafkaTemplate(factory: ProducerFactory<String, ByteArray>): KafkaTemplate<String, ByteArray> =
		KafkaTemplate(factory)

	/**
	 * 토픽을 미리 만들어 파티션 수를 고정한다. 자동 생성에 맡기면 환경마다 파티션 수가 달라져
	 * 순서 보장을 확인하는 테스트가 환경을 탄다. 토픽이 이미 있으면 브로커가 이 선언을 무시한다.
	 */
	@Bean
	fun docEventsTopic() = TopicBuilder.name(relayProperties.kafka.topic)
		.partitions(relayProperties.kafka.partitions)
		.replicas(1)
		.build()
}
