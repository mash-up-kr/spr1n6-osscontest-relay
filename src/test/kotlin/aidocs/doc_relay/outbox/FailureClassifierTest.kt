package aidocs.doc_relay.outbox

import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.junit.jupiter.api.Test
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
	fun `classifies everything else as transient`() {
		assertFalse(classifier.isPermanent(RuntimeException("broker down")))
	}
}
