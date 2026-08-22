package aidocs.doc_relay.outbox

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaProducerException
import java.util.concurrent.CompletionException
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class FailureClassifierTest {

	private val classifier = FailureClassifier()

	@Test
	fun `classifies envelope assembly failures as permanent`() {
		assertTrue(classifier.isPermanent(EnvelopeAssemblyException(RuntimeException("bad json"))))
	}

	@Test
	fun `classifies record too large as permanent`() {
		assertTrue(classifier.isPermanent(RecordTooLargeException("too big")))
	}

	@Test
	fun `classifies topic authorization failures as permanent`() {
		assertTrue(classifier.isPermanent(TopicAuthorizationException("nope")))
	}

	@Test
	fun `classifies invalid topic as permanent`() {
		assertTrue(classifier.isPermanent(InvalidTopicException("bad topic")))
	}

	@Test
	fun `classifies a wrapped permanent cause by unwrapping one level`() {
		// future.join() 은 실제 원인 예외를 CompletionException 으로 한 겹 감싸므로,
		// 그렇게 감싸진 예외도 벗겨서(unwrap) 분류할 수 있어야 한다.
		assertTrue(classifier.isPermanent(CompletionException(RecordTooLargeException("too big"))))
	}

	@Test
	fun `classifies a permanent cause wrapped by the async producer callback path`() {
		// 브로커가 거절한 실패는 두 겹이 덧씌워져 온다 — KafkaTemplate 의 콜백이
		// KafkaProducerException 으로, future.join() 이 CompletionException 으로.
		// 한 겹만 벗기던 시절에는 여기서 false 가 나와 재시도를 5회 다 태우고 DEAD 로 갔다가
		// 자동 복구가 다시 살려내는 순환에 빠졌다.
		val brokerRejected = CompletionException(
			KafkaProducerException(
				ProducerRecord("doc.events.v1", "1", ByteArray(0)),
				"Failed to send",
				RecordTooLargeException("The message is 1048625 bytes when serialized"),
			)
		)

		assertTrue(classifier.isPermanent(brokerRejected))
	}

	@Test
	fun `stops instead of looping when the cause chain is cyclic`() {
		// cause 가 서로를 가리키는 예외를 만나도 끝까지 훑는 로직이 멈춰야 한다.
		val a = RuntimeException("a")
		val b = RuntimeException("b", a)
		a.initCause(b)

		assertFalse(classifier.isPermanent(a))
	}

	@Test
	fun `classifies everything else as transient`() {
		assertFalse(classifier.isPermanent(RuntimeException("broker down")))
	}
}
