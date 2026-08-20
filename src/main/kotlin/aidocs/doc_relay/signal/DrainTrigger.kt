package aidocs.doc_relay.signal

import aidocs.doc_relay.RelayProperties
import aidocs.doc_relay.outbox.OutboxDrainer
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * "발행할 이벤트가 생겼다"는 신호를 한곳으로 모으고, 전용 스레드 하나가 그 신호를 받아
 * 드레인 사이클을 돌린다. 신호를 보내는 쪽은 셋이다 — DB 알림 리스너, 폴링 스케줄러, 어드민.
 *
 * 신호가 담는 정보는 "다음 사이클이 필요하다" 하나뿐이라 용량 1짜리 큐로 충분하다.
 * 큐가 이미 차 있으면 새 신호는 버려지므로, 신호가 1000개 몰려와도 추가 사이클은 최대 한 번이다.
 * 사이클은 한 번에 하나만 돈다. 이 서버에서 동시성을 신경 써야 하는 지점은 여기뿐이다.
 *
 * 스레드는 생성자가 아니라 start() 에서 띄운다. 생성자에서 띄우면 아직 초기화가 끝나지 않은
 * 객체를 새 스레드가 참조하게 되어 필드 값이 온전히 보이는지 보장할 수 없다. 스프링은
 * 빈 생성과 의존성 주입이 모두 끝난 뒤에만 start() 를 부른다.
 */
@Component
class DrainTrigger(
	private val drainer: OutboxDrainer,
	private val properties: RelayProperties,
) : SmartLifecycle {

	private val log = LoggerFactory.getLogger(javaClass)
	private val pending = ArrayBlockingQueue<Unit>(1)
	private val running = AtomicBoolean(false)
	private val draining = AtomicBoolean(false)
	private val shuttingDown = AtomicBoolean(false)
	private var worker: Thread? = null

	/** 신호를 보낸다. 종료가 시작된 뒤에는 조용히 무시한다. */
	fun signal() {
		if (shuttingDown.get()) return
		pending.offer(Unit)
	}

	/** 테스트용. 대기 중인 신호와 진행 중인 사이클이 모두 없어지면 true 를 돌려준다. */
	fun awaitIdle(timeoutMillis: Long): Boolean {
		val deadline = System.currentTimeMillis() + timeoutMillis
		while (System.currentTimeMillis() < deadline) {
			if (pending.isEmpty() && !draining.get()) {
				Thread.sleep(100)
				if (pending.isEmpty() && !draining.get()) return true
			}
			Thread.sleep(50)
		}
		return false
	}

	override fun start() {
		if (!running.compareAndSet(false, true)) return
		shuttingDown.set(false)
		worker = Thread({ loop() }, "outbox-drainer").apply {
			isDaemon = true
			start()
		}
	}

	/**
	 * 새 신호를 먼저 막고, 진행 중인 사이클이 끝나기를 [상한][RelayProperties.Shutdown.drainTimeout]
	 * 안에서 기다린 뒤에야 스레드를 끊는다. 그래야 배포마다 좀비 타임아웃 + 스캔 주기가
	 * 붙는 것을 피한다. 상한을 넘기면 경고를 남기고 그대로 진행한다 — 무한히
	 * 기다리면 배포 자체가 멈춘다. 못 끝낸 행은 좀비 회수가 안전망으로 처리한다.
	 */
	override fun stop() {
		shuttingDown.set(true)
		val deadline = System.currentTimeMillis() + properties.shutdown.drainTimeout.toMillis()
		while (draining.get() && System.currentTimeMillis() < deadline) {
			try {
				Thread.sleep(100)
			} catch (e: InterruptedException) {
				Thread.currentThread().interrupt()
				break
			}
		}
		if (draining.get()) {
			log.warn(
				"드레인 대기 상한({}) 안에 진행 중이던 사이클이 끝나지 않았다. 좀비 회수가 나머지를 처리한다.",
				properties.shutdown.drainTimeout,
			)
		}
		running.set(false)
		worker?.interrupt()
	}

	// 스레드가 실제로 살아 있는지도 함께 본다. loop() 가 OutOfMemoryError 같은 Error 로 죽으면
	// 스레드는 빠져나가지만 running 플래그는 stop() 에서만 내려가므로 계속 true 로 남는다.
	// 그러면 헬스체크가 이미 멈춘 드레이너를 정상이라고 보고하고, 릴레이는 살아 있는 채로
	// 아무 이벤트도 발행하지 않는다.
	override fun isRunning(): Boolean = running.get() && worker?.isAlive == true

	private fun loop() {
		while (running.get()) {
			try {
				if (pending.poll(1, TimeUnit.SECONDS) == null) continue
				draining.set(true)
				drainer.drainUntilEmpty { shuttingDown.get() }
			} catch (e: InterruptedException) {
				Thread.currentThread().interrupt()
				return
			} catch (e: Exception) {
				// 사이클 하나가 실패해도 루프는 계속 돌아야 한다. 다음 신호에 다시 시도한다.
				// 여기서 빠져나가면 릴레이가 살아 있는 채로 발행을 멈춘다.
				log.error("드레인 사이클 실패", e)
			} finally {
				draining.set(false)
			}
		}
	}
}
