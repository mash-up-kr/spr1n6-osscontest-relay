package aidocs.doc_relay.outbox

import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.format.DateTimeFormatter

/**
 * DB 에서 읽은 행을 Kafka 로 보낼 메시지 본문으로 만든다.
 *
 * 세 가지를 지킨다. 셋 다 "릴레이가 이벤트의 내용을 몰라도 되게 한다"는 같은 목적이다.
 *
 *   1. payload 는 들여다보지 않고 그대로 옮긴다. 무슨 필드가 들어 있는지 알 필요가 없으므로,
 *      API 서버 쪽에서 필드를 추가해도 이 클래스는 바뀌지 않는다.
 *   2. eventType 은 컬럼 값을 그대로 복사하고 종류에 따라 분기하지 않는다. 새 이벤트 종류가
 *      생겨도 손댈 곳이 없다.
 *   3. 바깥쪽 필드는 컬럼에서 가져온다. payload 안에 있는 값을 꺼내 올리지 않는다.
 *      두 출처를 섞기 시작하면 payload 의 형태를 알아야 하는 코드가 생긴다.
 */
@Component
class EnvelopeAssembler(private val mapper: ObjectMapper) {

	fun assemble(row: OutboxEventRow): ByteArray {
		val envelopeNode = mapper.createObjectNode().apply {
			put("eventId", row.id.toString())
			put("tenantId", row.tenantId)
			put("documentId", row.documentId)
			// 컬럼이 NULL 이면 필드를 빼지 않고 null 로 넣는다. 필드의 유무가 이벤트 종류에 따라
			// 달라지면 받는 쪽이 종류별로 분기해야 하고, 그건 이 클래스가 피하려는 바로 그것이다.
			row.documentVersionId?.let { put("documentVersionId", it) }
				?: putNull("documentVersionId")
			put("eventType", row.eventType)
			put("schemaVersion", row.eventSchemaVersion)
			put("occurredAt", DateTimeFormatter.ISO_INSTANT.format(row.createdAt))
			set("payload", mapper.readTree(row.payload))
		}
		return mapper.writeValueAsBytes(envelopeNode)
	}
}
