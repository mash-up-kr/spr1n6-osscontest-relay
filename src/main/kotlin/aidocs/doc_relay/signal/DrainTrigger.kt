package aidocs.doc_relay.signal

import aidocs.doc_relay.outbox.OutboxDrainer
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * spec §5. LISTEN·폴링·어드민이 전부 여기로 신호를 쏘고, 드레인 스레드 하나가 사이클을 돈다.
 *
 * 신호는 "다음 사이클이 필요하다"는 사실 하나뿐이다. 용량 1짜리 큐가 그 사실을 담으므로
 * 신호가 1000개 와도 추가 드레인은 최대 1회다. 이 서버의 동시성 지점은 여기 하나다.
 *
 * 워커 스레드는 생성자 프로퍼티 초기화 시점이 아니라 SmartLifecycle.start() 에서 기동한다.
 * 생성자 안에서 start() 를 호출하면 빈이 완전히 초기화되기 전에 스레드가 this 를 참조하며
 * loop() 를 실행할 수 있어(this 이스케이프) 필드 값이 온전히 보이는지 보장할 수 없다.
 * Spring 컨테이너는 빈 생성·의존성 주입이 끝난 뒤에만 start() 를 호출하므로 이 경합이 사라진다.
 */
@Component
class DrainTrigger(private val drainer: OutboxDrainer) : SmartLifecycle {

	private val log = LoggerFactory.getLogger(javaClass)
	private val pending = ArrayBlockingQueue<Unit>(1)
	private val running = AtomicBoolean(false)
	private val draining = AtomicBoolean(false)
	private var worker: Thread? = null

	/** 논블로킹. 이미 신호가 대기 중이면 아무 일도 하지 않는다. */
	fun signal() {
		pending.offer(Unit)
	}

	/** 테스트용. 대기 신호와 진행 중 사이클이 모두 소진되면 true. */
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
		worker = Thread({ loop() }, "outbox-drainer").apply {
			isDaemon = true
			start()
		}
	}

	override fun stop() {
		running.set(false)
		worker?.interrupt()
	}

	// worker?.isAlive 도 확인한다: loop() 가 Error(OOM, StackOverflow 등)로 죽으면 스레드는
	// 빠져나가지만 running 은 stop() 에서만 내려가므로 running 만 보면 죽은 뒤에도 계속
	// true 를 돌려준다 — 헬스체크가 실제로는 멈춘 드레이너를 계속 정상이라고 보고하게 된다.
	override fun isRunning(): Boolean = running.get() && worker?.isAlive == true

	private fun loop() {
		while (running.get()) {
			try {
				if (pending.poll(1, TimeUnit.SECONDS) == null) continue
				draining.set(true)
				drainer.drainUntilEmpty()
			} catch (e: InterruptedException) {
				Thread.currentThread().interrupt()
				return
			} catch (e: Exception) {
				// 사이클 하나가 죽어도 루프는 살아 있어야 한다. 다음 신호에 다시 시도한다.
				log.error("드레인 사이클 실패", e)
			} finally {
				draining.set(false)
			}
		}
	}
}
