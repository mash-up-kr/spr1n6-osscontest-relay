package aidocs.doc_relay

import aidocs.doc_relay.signal.DrainTrigger
import aidocs.doc_relay.signal.PgNotificationListener
import org.springframework.boot.health.contributor.AbstractHealthIndicator
import org.springframework.boot.health.contributor.Health
import org.springframework.context.annotation.Lazy
import org.springframework.stereotype.Component

/**
 * 드레인 스레드가 살아 있는지를 liveness(파드 재시작 여부를 결정하는 헬스체크)에 연결한다.
 * 이 서버는 HTTP 요청을 받지 않으므로 헬스체크와 지표가 문제를 알아채는 유일한 경로다 —
 * 여기 연결하지 않으면 릴레이가 살아 있는 채로 아무 일도 하지 않는 상태를 아무도 모른다.
 *
 * 빈 이름이 "drainerHealthIndicator"가 되어 헬스 컨트리뷰터 이름이 "drainer"로 잡히므로
 * application.yaml 의 liveness/readiness 그룹 include 와 이름이 맞아떨어진다.
 *
 * LISTEN 커넥션이 끊긴 것은 DOWN 이 아니다 — 폴링이 덮으므로 상세에만 싣는다. 감시는
 * relay.listener.connected 게이지가 한다.
 */
@Component
class DrainerHealthIndicator(
	private val drainTrigger: DrainTrigger,
	@Lazy private val listener: PgNotificationListener,
) : AbstractHealthIndicator() {

	override fun doHealthCheck(builder: Health.Builder) {
		builder.withDetail("listenerConnected", listener.connected)
		builder.withDetail("drainerRunning", drainTrigger.isRunning)
		if (drainTrigger.isRunning) {
			builder.up()
		} else {
			builder.down()
		}
	}
}
