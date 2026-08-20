package aidocs.doc_relay.outbox

import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.springframework.stereotype.Component

/**
 * 발행 실패를 영구/일시로 나눈다. 다시 시도해도 결과가 달라지지 않는 실패만
 * 영구로 분류한다 — 확신이 없으면 일시로 둔다. 잘못 재시도하면 낭비지만, 잘못 영구로
 * 판정하면 멈춰선 안 될 이벤트가 멈춘다.
 *
 * 예외가 직접 온 경우와 CompletableFuture.join() 이 CompletionException 으로 감싼 경우를
 * 모두 다루기 위해 예외 자신과 그 cause 를 한 단계까지 본다.
 */
@Component
class FailureClassifier {

	fun isPermanent(exception: Throwable): Boolean =
		isPermanentType(exception) || isPermanentType(exception.cause)

	private fun isPermanentType(t: Throwable?): Boolean = when (t) {
		is EnvelopeAssemblyException -> true
		is RecordTooLargeException -> true
		is TopicAuthorizationException -> true
		is InvalidTopicException -> true
		else -> false
	}
}
