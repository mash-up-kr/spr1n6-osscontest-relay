package aidocs.doc_relay.signal

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/**
 * 아무 일이 없어도 정해진 주기마다 드레인을 한 번 깨운다.
 *
 * DB 알림이 유실됐거나 리스너가 통째로 죽은 경우를 한꺼번에 덮는 안전망이다. 알림 경로가
 * 완전히 끊겨도 릴레이는 이 주기만큼 느려질 뿐 멈추지 않는다. 무엇을 발행할지는 어차피
 * 드레인 사이클이 DB 에 직접 묻기 때문에, 여기서는 깨우기만 하면 된다.
 */
@Component
class PollingScheduler(private val trigger: DrainTrigger) {

	@Scheduled(fixedDelayString = "\${relay.polling.interval}")
	fun poll() {
		trigger.signal()
	}
}
