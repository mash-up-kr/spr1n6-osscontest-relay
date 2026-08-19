package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.CompletableFuture

/**
 * 선점한 행들을 Kafka 로 보내고 성공한 것과 실패한 것을 갈라 돌려준다.
 *
 * 한 건씩 보내고 응답을 기다리는 대신, 배치 전체를 먼저 밀어 넣고 한 번에 응답을 모은다.
 * 100건이면 왕복 100번이 아니라 한 번이다.
 *
 * 이 호출은 DB 트랜잭션 밖에서 일어난다. 행에 락을 건 채로 네트워크를 기다리면 Kafka 가
 * 느려질 때 DB 락이 그만큼 오래 잡힌다.
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

		// 메시지를 만드는 과정에서 예외가 날 수 있다. payload 가 깨져 있으면 봉투 조립이 실패한다.
		// 그 예외를 여기서 잡지 않으면 문제가 있는 행 하나 때문에 뒤에 있는 멀쩡한 행들이
		// 보내지지도 못하고, 결과를 돌려주는 대신 예외로 끝나 이번 사이클 전체가 날아간다.
		// 행마다 따로 감싸서 실패한 행만 실패 목록으로 보내고 나머지는 그대로 보낸다.
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
		// 메시지 키를 document_id 로 둔다. 같은 키는 항상 같은 파티션으로 가고 파티션 안에서는
		// 순서가 지켜지므로, 한 문서의 이벤트들이 보낸 순서대로 도착한다. 키를 바꾸면 같은
		// 문서의 새 버전과 옛 버전이 다른 파티션으로 흩어져 순서가 뒤집힐 수 있다.
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
