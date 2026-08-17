package aidocs.doc_relay.signal

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/**
 * spec §5. NOTIFY 유실과 리스너 다운을 동시에 덮는 안전망.
 * 리스너가 완전히 죽어도 릴레이는 이 주기만큼의 지연으로 계속 동작한다.
 */
@Component
class PollingScheduler(private val trigger: DrainTrigger) {

	@Scheduled(fixedDelayString = "\${relay.polling.interval}")
	fun poll() {
		trigger.signal()
	}
}
