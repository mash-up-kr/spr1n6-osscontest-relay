package aidocs.doc_relay.outbox

import aidocs.doc_relay.support.RelayIntegrationTest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.test.context.TestPropertySource
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 브로커가 거절한 영구 실패도 재시도 없이 DEAD 로 가야 한다.
 *
 * [FailureClassificationDrainTest] 와 비슷해 보이지만 타는 경로가 다르다. 그쪽은 payload 를
 * 프로듀서의 max.request.size 보다 크게 만들어, 네트워크로 나가기 전에 클라이언트가 걸러내게 한다.
 * 그 경우 예외는 한 겹만 싸여 오므로 cause 를 한 단계만 봐도 분류가 됐다.
 *
 * 여기서는 클라이언트 상한은 넘지 않고 브로커의 max.message.bytes 만 넘겨서, 실패가 프로듀서
 * 콜백을 통해 비동기로 돌아오게 한다. 이 경로는 KafkaTemplate 이 KafkaProducerException 으로,
 * future.join() 이 CompletionException 으로 두 겹을 덧씌운다 — 한 단계만 벗기던 시절에는
 * 영구 실패가 일시 실패로 분류돼 재시도를 다 태우고 DEAD 로 갔다가 자동 복구가 다시 살려내는
 * 순환에 빠졌다.
 *
 * 공유 컨테이너의 doc.events.v1 설정을 건드리면 다른 테스트에 영향이 가므로 이 클래스 전용
 * 토픽을 쓴다.
 */
@TestPropertySource(properties = ["relay.kafka.topic=broker-limit-events"])
class BrokerRejectionClassificationTest : RelayIntegrationTest() {

	@Autowired private lateinit var drainer: OutboxDrainer
	@Autowired private lateinit var kafkaAdmin: KafkaAdmin

	/** 브로커가 받아 줄 최대 크기. 프로듀서 상한(기본 1MB)보다 훨씬 작게 둬야 브로커가 거절한다. */
	private val brokerLimitBytes = 1_000

	@BeforeEach
	fun shrinkBrokerLimit() {
		AdminClient.create(kafkaAdmin.configurationProperties).use { admin ->
			val topic = ConfigResource(ConfigResource.Type.TOPIC, "broker-limit-events")
			admin.incrementalAlterConfigs(
				mapOf(
					topic to listOf(
						AlterConfigOp(
							ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, brokerLimitBytes.toString()),
							AlterConfigOp.OpType.SET,
						)
					)
				)
			).all().get()
		}
	}

	@Test
	fun `a message the broker rejects goes dead on the first attempt without retrying`() {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		val id = jdbc.sql("SELECT id FROM outbox_event WHERE document_version_id = :v")
			.param("v", versionId).query(UUID::class.java).single()
		// 브로커 상한은 넘지만 프로듀서 상한(1MB)에는 한참 못 미치는 크기.
		jdbc.sql("UPDATE outbox_event SET payload = jsonb_build_object('pad', repeat('a', 5000)) WHERE id = :id")
			.param("id", id).update()

		drainer.drainOnce()

		assertEquals("DEAD", statusOf(id))
		assertEquals(
			1,
			jdbc.sql("SELECT publish_attempt_count FROM outbox_event WHERE id = :id")
				.param("id", id).query(Int::class.java).single(),
			"영구 실패는 재시도 없이 1회 만에 DEAD 여야 한다",
		)
		assertTrue(
			jdbc.sql("SELECT next_attempt_at = 'infinity' FROM outbox_event WHERE id = :id")
				.param("id", id).query(Boolean::class.java).single(),
			"영구 실패는 곧바로 정지된 DEAD(held) 여야 한다",
		)

		// 포장 예외의 "Failed to send" 가 아니라 진짜 원인이 남아야 DEAD 목록을 보고 조치할 수 있다.
		val error = jdbc.sql("SELECT last_error_message FROM outbox_event WHERE id = :id")
			.param("id", id).query(String::class.java).single()
		assertTrue(
			error.contains("larger than", ignoreCase = true) || error.contains("bytes", ignoreCase = true),
			"원인(크기 초과)이 드러나야 한다. 실제 값: $error",
		)
	}
}
