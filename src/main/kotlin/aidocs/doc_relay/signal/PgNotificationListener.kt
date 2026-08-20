package aidocs.doc_relay.signal

import aidocs.doc_relay.RelayProperties
import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

/**
 * DB가 보내는 알림을 받아 드레인 사이클을 깨운다. 새 이벤트가 생긴 것을 가장 빨리 아는 경로이고,
 * 덕분에 업로드 후 1초 안에 발행이 시작된다.
 *
 * 커넥션 풀에서 커넥션을 빌리지 않고 전용 커넥션을 따로 연다. 알림을 기다리는 동안 커넥션을
 * 계속 점유하기 때문에, 풀에서 빌리면 다른 쿼리가 쓸 커넥션을 하나 영구히 뺏는 셈이 된다.
 *
 * 알림에 실려 오는 이벤트 UUID 는 조회 키로 쓰지 않는다. 알림은 "일어나라"는 뜻일 뿐이고,
 * 무엇을 발행할지는 항상 DB 에 다시 물어본다. 알림을 정보의 출처로 삼으면 알림이 유실된
 * 이벤트는 영원히 발행되지 않는다.
 *
 * 재연결에 성공하면 조건 없이 신호를 한 번 보낸다. 끊겨 있던 동안 놓친 알림을 이 한 번이 전부
 * 보상한다.
 *
 * 접속 정보를 DataSourceProperties 가 아니라 JdbcConnectionDetails 로 받는 이유: 이 프로젝트는
 * application.yaml 에 접속 정보를 두지 않는다. 테스트에서는 Testcontainers 가 컨테이너 주소를
 * JdbcConnectionDetails 빈으로 공급하고, 운영에서는 설정값을 읽는 기본 구현이 등록된다.
 * DataSourceProperties 를 직접 읽으면 테스트에서 주소가 비어 있어 바로 실패한다.
 */
@Component
class PgNotificationListener(
	private val connectionDetails: JdbcConnectionDetails,
	private val relayProperties: RelayProperties,
	private val trigger: DrainTrigger,
) : SmartLifecycle {

	private val log = LoggerFactory.getLogger(javaClass)
	private val running = AtomicBoolean(false)
	private val connectedFlag = AtomicBoolean(false)
	private val reconnects = AtomicLong(0)
	private var worker: Thread? = null

	val connected: Boolean get() = connectedFlag.get()
	val reconnectCount: Long get() = reconnects.get()

	override fun start() {
		if (!relayProperties.listener.enabled) {
			log.info("relay.listener.enabled=false 이므로 리스너를 시작하지 않는다")
			return
		}
		if (!running.compareAndSet(false, true)) return
		worker = Thread({ loop() }, "outbox-listener").apply {
			isDaemon = true
			start()
		}
	}

	override fun stop() {
		running.set(false)
		worker?.interrupt()
	}

	override fun isRunning(): Boolean = running.get()

	private fun loop() {
		var backoffMillis = relayProperties.listener.reconnectBase.toMillis()
		while (running.get()) {
			try {
				openConnection().use { connection ->
					connection.createStatement().use { it.execute("LISTEN ${relayProperties.listener.channel}") }
					connectedFlag.set(true)
					backoffMillis = relayProperties.listener.reconnectBase.toMillis()

					// 연결되자마자 무조건 한 번 깨운다. 끊겨 있던 동안 놓친 알림을 여기서 보상한다.
					trigger.signal()

					val pg = connection.unwrap(PGConnection::class.java)
					while (running.get() && !connection.isClosed) {
						val notifications = pg.getNotifications(NOTIFICATION_POLL_MILLIS)
						if (!notifications.isNullOrEmpty()) {
							// 알림 내용은 읽지 않는다. 몇 건이 왔든 깨우기만 하고,
							// 무엇을 발행할지는 드레인 사이클이 DB 에 다시 묻는다.
							trigger.signal()
						}
					}
				}
			} catch (e: InterruptedException) {
				Thread.currentThread().interrupt()
				return
			} catch (e: Exception) {
				if (!running.get()) return
				log.warn("LISTEN 커넥션이 끊겼다. {}ms 뒤 재연결한다: {}", backoffMillis, e.message)
			} finally {
				connectedFlag.set(false)
			}

			if (!running.get()) return
			reconnects.incrementAndGet()
			try {
				Thread.sleep(backoffMillis)
			} catch (e: InterruptedException) {
				Thread.currentThread().interrupt()
				return
			}
			backoffMillis = min(backoffMillis * 2, relayProperties.listener.reconnectMax.toMillis())
		}
	}

	private fun openConnection(): Connection {
		val props = Properties().apply {
			setProperty("user", connectionDetails.username)
			setProperty("password", connectionDetails.password)
			// 이름을 붙여 두면 테스트나 운영에서 이 커넥션만 골라 끊을 수 있다.
			// 재연결 경로를 검증할 때 쓴다.
			setProperty("ApplicationName", APPLICATION_NAME)
		}
		return DriverManager.getConnection(connectionDetails.jdbcUrl, props)
	}

	companion object {
		const val APPLICATION_NAME = "doc-relay-listener"
		private const val NOTIFICATION_POLL_MILLIS = 1_000
	}
}
