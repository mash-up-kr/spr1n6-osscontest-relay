package aidocs.doc_relay.outbox

import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.springframework.stereotype.Component

/** cause 가 순환하는 예외를 만나도 멈추기 위한 상한. 실제 포장은 서너 겹을 넘지 않는다. */
private const val MAX_CAUSE_DEPTH = 20

/**
 * 예외 자신에서 시작해 cause 를 따라 끝까지 훑는다.
 *
 * 몇 겹으로 싸여 있는지 세지 않는 것이 요점이다. 포장 깊이는 우리가 정하는 값이 아니라 라이브러리
 * 사정이라, 같은 실패도 어느 경로로 오느냐에 따라 깊이가 다르다 — 프로듀서가 보내기 전에 걸러낸
 * 실패는 한 겹이지만, 브로커가 거절해 콜백으로 오는 실패는 KafkaTemplate 이 KafkaProducerException
 * 으로 한 겹, future.join() 이 CompletionException 으로 또 한 겹을 덧씌운다. 깊이를 고정해 두면
 * 그 사정이 바뀌는 순간 오류 없이 조용히 어긋난다.
 *
 * cause 가 자기 자신을 가리키는 경우를 걸러 내고 [MAX_CAUSE_DEPTH] 로 잘라 무한 순회를 막는다.
 */
internal fun Throwable.causeChain(): Sequence<Throwable> =
	generateSequence(this) { current -> current.cause?.takeIf { it !== current } }
		.take(MAX_CAUSE_DEPTH)

/**
 * 발행 실패를 영구/일시로 나눈다. 다시 시도해도 결과가 달라지지 않는 실패만
 * 영구로 분류한다 — 확신이 없으면 일시로 둔다. 잘못 재시도하면 낭비지만, 잘못 영구로
 * 판정하면 멈춰선 안 될 이벤트가 멈춘다.
 */
@Component
class FailureClassifier {

	fun isPermanent(exception: Throwable): Boolean =
		exception.causeChain().any(::isPermanentType)

	private fun isPermanentType(t: Throwable): Boolean = when (t) {
		is EnvelopeAssemblyException -> true
		is RecordTooLargeException -> true
		is TopicAuthorizationException -> true
		is InvalidTopicException -> true
		else -> false
	}
}
