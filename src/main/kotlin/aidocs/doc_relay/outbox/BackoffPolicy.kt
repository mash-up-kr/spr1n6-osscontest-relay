package aidocs.doc_relay.outbox

import aidocs.doc_relay.RelayProperties
import org.springframework.stereotype.Component

/**
 * 발행에 실패했을 때 다음 시도까지 얼마나 기다릴지 정한다. 실패를 거듭할수록 두 배씩 늘리되
 * 상한을 둔다 — n 번째 실패 후 대기 시간은 `min(기본값 × 2의 (n-1)승, 상한)` 이다.
 *
 * 계산은 이 클래스가 아니라 SQL 이 한다([sqlWith]). 실패한 행마다 지금까지의 실패 횟수가
 * 다르기 때문이다. 여기서 값을 하나 계산해 넘기면 배치 안의 모든 행이 같은 대기 시간을
 * 갖게 되고, 제대로 하려면 행마다 UPDATE 를 따로 날려야 한다.
 */
@Component
class BackoffPolicy(properties: RelayProperties) {

	val baseSeconds: Double = properties.backoff.base.toMillis() / 1000.0
	val maxSeconds: Double = properties.backoff.max.toMillis() / 1000.0

	companion object {
		/**
		 * 대기 시간을 초 단위로 계산하는 SQL 식.
		 *
		 * `:baseSeconds` 와 `:maxSeconds` 는 값을 넘겨받는 자리이고, `{attemptCount}` 는
		 * 컬럼 이름이 들어갈 자리다. 후자를 값으로 넘기지 않는 이유는 행마다 실패 횟수가
		 * 달라야 하기 때문이다. 값으로 넘기면 배치의 모든 행이 같은 대기 시간을 갖는다.
		 */
		private const val TEMPLATE = "LEAST(:baseSeconds * POWER(2, {attemptCount}), :maxSeconds)"

		/**
		 * 실패 횟수가 담긴 컬럼 이름을 끼워 넣어 완성된 식을 돌려준다.
		 *
		 * 쓰는 쪽에서 결과에 `* INTERVAL '1 second'` 를 곱해 시간 간격으로 만들고,
		 * `:baseSeconds` 와 `:maxSeconds` 두 값을 넘겨야 한다.
		 */
		fun sqlWith(attemptCountColumn: String): String =
			TEMPLATE.replace("{attemptCount}", attemptCountColumn)
	}
}
