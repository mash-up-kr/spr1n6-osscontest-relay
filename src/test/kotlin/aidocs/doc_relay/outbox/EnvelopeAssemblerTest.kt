package aidocs.doc_relay.outbox

import org.junit.jupiter.api.Test
import tools.jackson.databind.ObjectMapper
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals

class EnvelopeAssemblerTest {

	private val mapper = ObjectMapper()
	private val assembler = EnvelopeAssembler(mapper)

	private fun row(
		eventType: String = "INDEXING_REQUESTED",
		payload: String = """{"versionNo":3,"sourceObjectKey":"t/d/v3.pdf"}""",
	) = OutboxEventRow(
		id = UUID.fromString("0193f2a1-0000-7000-8000-000000000001"),
		tenantId = 1,
		documentId = 42,
		documentVersionId = 137,
		eventType = eventType,
		eventSchemaVersion = 1,
		payload = payload,
		traceId = "0af7651916cd43dd",
		publishAttemptCount = 0,
		createdAt = Instant.parse("2026-08-13T09:14:22Z"),
		lockedAt = Instant.now(),
	)

	@Test
	fun `restores top level fields from columns`() {
		val node = mapper.readTree(assembler.assemble(row()))

		assertEquals("0193f2a1-0000-7000-8000-000000000001", node["eventId"].asString())
		assertEquals(1, node["tenantId"].asInt())
		assertEquals(42, node["documentId"].asInt())
		assertEquals(137, node["documentVersionId"].asInt())
		assertEquals(1, node["schemaVersion"].asInt())
		// 최상위 occurredAt 은 outbox_event.created_at 에서 온다.
		assertEquals("2026-08-13T09:14:22Z", node["occurredAt"].asString())
	}

	@Test
	fun `passes payload through without parsing or reshaping`() {
		val payload = """{"versionNo":3,"nested":{"deep":[1,2,3]},"unknownFutureField":"ok"}"""
		val node = mapper.readTree(assembler.assemble(row(payload = payload)))

		assertEquals(mapper.readTree(payload), node["payload"])
	}

	@Test
	fun `copies event type without branching`() {
		// DOCUMENT_DELETED 처럼 새 이벤트 타입이 CHECK 제약(허용값을 제한하는 DB 제약 조건)에
		// 추가돼도 릴레이 코드는 분기 없이 그대로 통과시켜야 한다.
		val node = mapper.readTree(assembler.assemble(row(eventType = "DOCUMENT_DELETED")))
		assertEquals("DOCUMENT_DELETED", node["eventType"].asString())
	}
}
