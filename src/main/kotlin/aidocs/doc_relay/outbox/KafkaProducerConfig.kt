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
 * spec §7. acks=all + 멱등 프로듀서만 켠다. Kafka 트랜잭션은 쓰지 않는다.
 *
 * 값을 ByteArray 로 두는 이유: 봉투 JSON 은 EnvelopeAssembler 가 이미 바이트로 만들었고,
 * 여기서 다시 직렬화기를 태우면 payload 원문 통과 원칙이 깨질 수 있다.
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
		// @ServiceConnection 은 KafkaConnectionDetails 빈으로 주소를 공급한다.
		// KafkaProperties 만 보면 테스트에서 컨테이너가 아니라 localhost:9092 를 가리킨다.
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

	/** 테스트와 로컬에서 파티션 수를 결정론적으로 만든다. 이미 있으면 브로커가 무시한다. */
	@Bean
	fun docEventsTopic() = TopicBuilder.name(relayProperties.kafka.topic)
		.partitions(relayProperties.kafka.partitions)
		.replicas(1)
		.build()
}
