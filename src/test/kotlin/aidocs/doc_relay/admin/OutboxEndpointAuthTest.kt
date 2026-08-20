package aidocs.doc_relay.admin

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * 127.0.0.1 바인딩만으로는 같은 호스트의 다른 프로세스가 접근하는 것까지는 막지 못하므로
 * 쓰기 액션에 최소한의 공유 비밀 인증을 둔다. relay.admin.token 이 비어 있으면(기본값)
 * 인증은 비활성이다 — 로컬/데모 환경에서 기존 호출부를 그대로 두기 위함이고, 운영 환경은
 * 이 값을 시크릿으로 채운다.
 */
@TestPropertySource(
	properties = [
		"relay.polling.interval=1h",
		"relay.zombie.scan-interval=1h",
		"relay.dead.recovery-scan-interval=1h",
		"relay.admin.token=correct-token",
	]
)
class OutboxEndpointAuthTest : RelayIntegrationTest() {

	@Autowired private lateinit var endpoint: OutboxEndpoint

	private fun deadRow(): java.util.UUID {
		val documentId = seedParents()
		val versionId = insertVersion(documentId)
		jdbc.sql("DELETE FROM outbox_event").update()
		return insertOutbox(
			documentId, versionId, status = "DEAD", attemptCount = 5,
			nextAttemptAt = Instant.now().plus(10, ChronoUnit.MINUTES),
		)
	}

	@Test
	fun `rejects a write action with a missing or wrong token`() {
		val id = deadRow()

		assertFailsWith<SecurityException> { endpoint.act(id.toString(), "RELEASE") }
		assertFailsWith<SecurityException> { endpoint.act(id.toString(), "RELEASE", "wrong-token") }
		assertEquals("DEAD", statusOf(id), "인증 실패는 행을 바꾸면 안 된다")
	}

	@Test
	fun `accepts a write action with the correct token`() {
		val id = deadRow()

		endpoint.act(id.toString(), "RELEASE", "correct-token")

		assertEquals(false, endpoint.byStatus("dead").single().held)
	}
}
