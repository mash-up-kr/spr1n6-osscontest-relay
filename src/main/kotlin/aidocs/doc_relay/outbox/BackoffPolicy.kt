package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import org.springframework.stereotype.Component
import java.time.Duration
import kotlin.math.min
import kotlin.math.pow

/**
 * spec §6 백오프: n 번째 실패 후 대기 = min(base × 2^(n-1), max).
 *
 * 실제 계산은 SQL 이 한다 ([SQL_EXPRESSION]). 행마다 publish_attempt_count 가 달라
 * 애플리케이션이 계산한 단일 값을 넘기면 배치 UPDATE 로 접히지 않기 때문이다.
 * 이 클래스의 [delayAfter] 는 같은 정의의 Kotlin 표현이며 로그와 테스트에 쓴다.
 */
@Component
class BackoffPolicy(properties: RelayProperties) {

	val baseSeconds: Double = properties.backoff.base.toMillis() / 1000.0
	val maxSeconds: Double = properties.backoff.max.toMillis() / 1000.0

	/**
	 * @param previousAttemptCount 증가 **전**의 publish_attempt_count.
	 *        n 번째 실패를 반영하는 시점의 컬럼 값이 n-1 이므로 그대로 넘기면 된다.
	 */
	fun delayAfter(previousAttemptCount: Int): Duration {
		val seconds = min(baseSeconds * 2.0.pow(previousAttemptCount), maxSeconds)
		return Duration.ofMillis((seconds * 1000).toLong())
	}

	companion object {
		/**
		 * SQL 에 그대로 끼워 넣는 초 단위 식. 결과에 `* INTERVAL '1 second'` 를 곱해 쓴다.
		 * `:attemptCount` 자리에는 보통 컬럼 `publish_attempt_count` 가 들어간다.
		 *
		 * `:attemptCount` 는 반드시 문자열 치환(`.replace(":attemptCount", ...)`)으로만
		 * 채워야 한다 — 절대 `.param()` 으로 바인딩하지 말 것. `.param()` 으로 바인딩하면
		 * 배치 UPDATE 의 모든 행이 파라미터로 넘긴 그 하나의 숫자로 계산된 동일한 백오프
		 * 값 하나로 뭉개진다 (행마다 다른 publish_attempt_count 를 못 쓰게 된다).
		 */
		const val SQL_EXPRESSION: String =
			"LEAST(:baseSeconds * POWER(2, :attemptCount), :maxSeconds)"
	}
}
