package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import aidocs.doc_relay.support.RelayIntegrationTest
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaPublisherTest : RelayIntegrationTest() {

	@Autowired private lateinit var publisher: KafkaPublisher
	@Autowired private lateinit var properties: RelayProperties

	private val mapper = ObjectMapper()

	private fun row(id: UUID = UUID.randomUUID(), documentId: Long = 42) = OutboxEventRow(
		id = id,
		tenantId = 1,
		documentId = documentId,
		documentVersionId = 137,
		eventType = "INDEXING_REQUESTED",
		eventSchemaVersion = 1,
		payload = """{"versionNo":3}""",
		traceId = "trace-1",
		publishAttemptCount = 0,
		createdAt = Instant.parse("2026-08-13T09:14:22Z"),
		lockedAt = Instant.now(),
	)

	/**
	 * RelayIntegrationTest 는 카프카 컨테이너를 JVM 전체에서 하나만 띄우는 싱글턴이라
	 * (companion object, 클래스마다 재기동하지 않음) 토픽도 테스트 전체가 공유한다.
	 * 그래서 "earliest" 오프셋에서 새 컨슈머 그룹으로 구독하면 앞서 실행된 다른 테스트
	 * 메서드가 같은 토픽에 남긴 레코드까지 함께 읽힌다 — 이 테스트 클래스 안에서도
	 * 기본 documentId=42 를 여러 메서드가 공유하므로 실제로 발생한다.
	 * 그래서 "몇 개가 오는가"가 아니라 "이번 호출이 만든 id 들만" 을 이번 발행분의
	 * eventId 헤더/봉투 값으로 걸러 세션 경계를 만든다. 순서 보장 검증에도 필요하다:
	 * 필터링 없이는 이전 테스트가 같은 documentId=42 로 이미 넣어 둔 레코드가 앞에 끼어
	 * "이번 배치의 순서" 를 검증할 수 없다.
	 */
	private fun consumeMatching(ids: Set<UUID>): List<Pair<String, ByteArray>> {
		val expected = ids.map { it.toString() }.toSet()
		val props = Properties().apply {
			put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
			put(ConsumerConfig.GROUP_ID_CONFIG, "test-${UUID.randomUUID()}")
			put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
			put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
			put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java)
		}
		KafkaConsumer<String, ByteArray>(props).use { consumer ->
			consumer.subscribe(listOf(properties.kafka.topic))
			val collected = mutableListOf<Pair<String, ByteArray>>()
			val deadline = System.currentTimeMillis() + 20_000
			while (collected.size < expected.size && System.currentTimeMillis() < deadline) {
				consumer.poll(Duration.ofMillis(500)).forEach { record ->
					val eventId = mapper.readTree(record.value())["eventId"].asString()
					if (eventId in expected) collected += record.key() to record.value()
				}
			}
			return collected
		}
	}

	@Test
	fun `publishes every row and reports them as succeeded`() {
		val rows = (1..3).map { row() }

		val outcome = publisher.publish(rows)

		assertEquals(rows.map { it.id }.toSet(), outcome.succeeded.toSet())
		assertTrue(outcome.failed.isEmpty())
		assertEquals(3, consumeMatching(rows.map { it.id }.toSet()).size)
	}

	@Test
	fun `uses documentId as the partition key`() {
		val target = row(documentId = 42)
		publisher.publish(listOf(target))

		assertEquals("42", consumeMatching(setOf(target.id)).single().first)
	}

	@Test
	fun `sends the assembled envelope as the value`() {
		val id = UUID.randomUUID()
		publisher.publish(listOf(row(id = id)))

		val node = mapper.readTree(consumeMatching(setOf(id)).single().second)
		assertEquals(id.toString(), node["eventId"].asString())
		assertEquals(137, node["documentVersionId"].asInt())
	}

	@Test
	fun `empty input does nothing`() {
		val outcome = publisher.publish(emptyList())

		assertTrue(outcome.succeeded.isEmpty())
		assertTrue(outcome.failed.isEmpty())
	}

	@Test
	fun `a bad row does not abort the rest of the batch`() {
		// KafkaPublisher.kt:25-27 원래 코드는 rows.map { row -> row to kafkaTemplate.send(toRecord(row)) }
		// 였다. toRecord() 가 envelopeAssembler.assemble() 을 호출하므로 이 자체가 던질 수 있는데
		// map 은 원소 단위 격리가 없어 한 행에서 던지면 나머지 행은 send() 조차 안 되고 publish() 가
		// PublishOutcome 대신 예외로 끝난다. payload 를 깨뜨려 그 경로를 강제로 밟는다.
		val good1 = row()
		val bad = row().copy(payload = "not valid json")
		val good2 = row()

		val outcome = publisher.publish(listOf(good1, bad, good2))

		assertEquals(setOf(good1.id, good2.id), outcome.succeeded.toSet())
		assertEquals(setOf(bad.id), outcome.failed.values.flatten().toSet())
		assertEquals(2, consumeMatching(setOf(good1.id, good2.id)).size)
	}

	@Test
	fun `same document id lands on one partition in order`() {
		// 파티션 키가 documentId 인 것만으로는 부족하다 — 같은 키의 여러 건이 실제로 순서를
		// 유지하는지까지 확인해야 한다.
		val rows = (1..10).map { row(documentId = 42) }

		publisher.publish(rows)

		val consumed = consumeMatching(rows.map { it.id }.toSet())
		assertEquals(1, consumed.map { it.first }.toSet().size, "키가 하나여야 한다")
		val ids = consumed.map { mapper.readTree(it.second)["eventId"].asString() }
		assertEquals(rows.map { it.id.toString() }, ids, "순서가 유지되지 않았다")
	}

	@Test
	fun `an unparseable payload is classified as a permanent failure`() {
		// jsonb 컬럼은 저장 시점에 문법을 검증하므로 DB 를 거치는 행으로는 깨진 payload 를
		// 만들 수 없다 — publish() 에 직접 넘기는 행으로만 재현 가능하다 ("a bad row does not
		// abort the rest of the batch" 테스트가 이미 쓴 방식과 같다).
		val bad = row().copy(payload = "not valid json")

		val outcome = publisher.publish(listOf(bad))

		val group = outcome.failed.keys.single()
		assertTrue(group.permanent, "봉투 조립 실패는 영구 실패로 분류돼야 한다")
	}

	@Test
	fun `a message over the broker size limit is classified as a permanent failure`() {
		val big = row().copy(payload = """{"pad":"${"a".repeat(1_200_000)}"}""")

		val outcome = publisher.publish(listOf(big))

		val group = outcome.failed.keys.single()
		assertTrue(group.permanent, "브로커 크기 상한 초과는 영구 실패로 분류돼야 한다")
	}
}
