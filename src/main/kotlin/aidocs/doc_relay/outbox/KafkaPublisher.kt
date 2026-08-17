package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.CompletableFuture

/**
 * spec §4②. 배치 전체를 async send 로 밀고 flush() 한 번으로 ack 를 모은 뒤 분류한다.
 * 이 호출은 트랜잭션 밖에서 일어난다 — 행 잠금을 물고 네트워크를 기다리지 않는다.
 */
@Component
class KafkaPublisher(
	private val kafkaTemplate: KafkaTemplate<String, ByteArray>,
	private val envelopeAssembler: EnvelopeAssembler,
	private val properties: RelayProperties,
) {

	fun publish(rows: List<OutboxEventRow>): PublishOutcome {
		if (rows.isEmpty()) return PublishOutcome.EMPTY

		val succeeded = mutableListOf<UUID>()
		val failed = mutableMapOf<String, MutableList<UUID>>()

		// toRecord() 는 envelopeAssembler.assemble() 을 호출하므로 그 자체가 던질 수 있다.
		// map 은 원소 단위 격리가 없어 한 행에서 던지면 나머지 행은 send() 조차 안 되고
		// flush() 도 못 돈 채 publish() 가 PublishOutcome 대신 예외로 끝난다.
		// mapNotNull + try/catch 로 미리 걸러 failed 로 보내고 나머지 행은 계속 보낸다.
		val inFlight: List<Pair<OutboxEventRow, CompletableFuture<*>>> = rows.mapNotNull { row ->
			try {
				row to kafkaTemplate.send(toRecord(row))
			} catch (e: Exception) {
				failed.getOrPut(messageOf(e)) { mutableListOf() } += row.id
				null
			}
		}
		kafkaTemplate.flush()

		inFlight.forEach { (row, future) ->
			try {
				future.join()
				succeeded += row.id
			} catch (e: Exception) {
				failed.getOrPut(messageOf(e)) { mutableListOf() } += row.id
			}
		}
		return PublishOutcome(succeeded, failed)
	}

	private fun messageOf(e: Exception): String =
		(e.cause ?: e).let { it.message ?: it.javaClass.simpleName }

	private fun toRecord(row: OutboxEventRow): ProducerRecord<String, ByteArray> {
		// 키는 document_id. 문서 단위 순서를 파티션이 지킨다 (spec §7). 절대 바꾸지 않는다.
		val record = ProducerRecord(
			properties.kafka.topic,
			row.documentId.toString(),
			envelopeAssembler.assemble(row),
		)
		record.headers().add("eventId", row.id.toString().toByteArray(StandardCharsets.UTF_8))
		record.headers().add("schemaVersion", row.eventSchemaVersion.toString().toByteArray(StandardCharsets.UTF_8))
		row.traceId?.let { record.headers().add("traceId", it.toByteArray(StandardCharsets.UTF_8)) }
		return record
	}
}
