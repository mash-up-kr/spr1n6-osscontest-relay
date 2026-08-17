package aidocs.doc_relay.outbox

import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.format.DateTimeFormatter

/**
 * outbox_event 행에 봉투를 씌운다 (spec §7).
 *
 * 조립 원칙 셋:
 *  1. payload 는 파싱하지 않고 JSON 노드로 그대로 꽂는다.
 *     릴레이가 payload 스키마를 몰라도 되고, 파트너가 필드를 추가해도 여기는 안 바뀐다.
 *  2. eventType 은 컬럼 값을 그대로 복사하고 분기하지 않는다.
 *  3. 최상위 필드는 컬럼에서 복원한다. payload 안의 값을 끌어올리지 않는다.
 */
@Component
class EnvelopeAssembler(private val mapper: ObjectMapper) {

	fun assemble(row: OutboxEventRow): ByteArray {
		val envelope = mapper.createObjectNode()
		envelope.put("eventId", row.id.toString())
		envelope.put("tenantId", row.tenantId)
		envelope.put("documentId", row.documentId)
		envelope.put("documentVersionId", row.documentVersionId)
		envelope.put("eventType", row.eventType)
		envelope.put("schemaVersion", row.eventSchemaVersion)
		envelope.put("occurredAt", DateTimeFormatter.ISO_INSTANT.format(row.createdAt))
		envelope.set("payload", mapper.readTree(row.payload))
		return mapper.writeValueAsBytes(envelope)
	}
}
