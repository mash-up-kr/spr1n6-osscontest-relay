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
	private val failureClassifier: FailureClassifier,
) {

	fun publish(rows: List<OutboxEventRow>): PublishOutcome {
		if (rows.isEmpty()) return PublishOutcome.EMPTY

		val succeeded = mutableListOf<UUID>()
		val failed = mutableMapOf<FailureGroup, MutableList<UUID>>()

		val inFlight: List<Pair<OutboxEventRow, CompletableFuture<*>>> = rows.mapNotNull { row ->
			try {
				val record = try {
					toRecord(row)
				} catch (e: Exception) {
					throw EnvelopeAssemblyException(e)
				}
				row to kafkaTemplate.send(record)
			} catch (e: Exception) {
				addFailure(failed, e, row.id)
				null
			}
		}
		kafkaTemplate.flush()

		inFlight.forEach { (row, future) ->
			try {
				future.join()
				succeeded += row.id
			} catch (e: Exception) {
				addFailure(failed, e, row.id)
			}
		}
		return PublishOutcome(succeeded, failed)
	}

	private fun addFailure(failed: MutableMap<FailureGroup, MutableList<UUID>>, e: Exception, id: UUID) {
		val group = FailureGroup(messageOf(e), failureClassifier.isPermanent(e))
		failed.getOrPut(group) { mutableListOf() } += id
	}

	/**
	 * 실패 사유로 남길 문자열. 포장 예외가 아니라 맨 끝 원인의 메시지를 쓴다.
	 *
	 * 한 겹만 벗기면 브로커가 거절한 실패에서 KafkaTemplate 이 씌운 "Failed to send" 가 남는다.
	 * 그건 실패했다는 말일 뿐 왜 실패했는지가 아니라서, DEAD 목록을 열어도 원인을 알 수 없다.
	 */
	private fun messageOf(e: Exception): String =
		e.causeChain().last().let { it.message ?: it.javaClass.simpleName }

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
