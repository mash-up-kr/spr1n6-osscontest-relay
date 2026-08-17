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
 * spec §5. 커넥션 풀이 아닌 전용 커넥션으로 LISTEN 한다.
 *
 * 알림 페이로드(이벤트 UUID)는 **조회 키로 쓰지 않는다.** 신호는 드레인 사이클을 깨우기만 하고
 * 진실의 원천은 항상 DB 쿼리다. 알림을 원천으로 삼으면 유실된 알림의 이벤트가 영영 안 나간다.
 *
 * 재연결 직후 무조건 signal() 을 한 번 쏜다. 끊긴 동안 유실된 NOTIFY 를 이 한 번이 전부 보상한다.
 *
 * JdbcConnectionDetails 를 주입받는다 (DataSourceProperties 가 아니다): 이 프로젝트는
 * application.yaml 에 spring.datasource.* 를 두지 않는다 — 테스트에서는 Testcontainers 의
 * @ServiceConnection 이 JdbcConnectionDetails 빈으로 컨테이너 주소를 공급하고, 운영에서는
 * PropertiesJdbcConnectionDetails(DataSourceProperties 로 채움) 가 항상 폴백으로 등록된다.
 * DataSourceProperties 를 직접 쓰면 테스트에서 url 이 null 이 되어 즉시 실패한다.
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

					// 재연결 직후 전체 드레인 1회. 끊긴 동안 유실된 알림을 여기서 보상한다.
					trigger.signal()

					val pg = connection.unwrap(PGConnection::class.java)
					while (running.get() && !connection.isClosed) {
						val notifications = pg.getNotifications(NOTIFICATION_POLL_MILLIS)
						if (!notifications.isNullOrEmpty()) {
							// 페이로드는 읽지 않는다. 깨우기만 한다.
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
			// 테스트가 이 커넥션만 골라 끊을 수 있게 표시해 둔다.
			setProperty("ApplicationName", APPLICATION_NAME)
		}
		return DriverManager.getConnection(connectionDetails.jdbcUrl, props)
	}

	companion object {
		const val APPLICATION_NAME = "doc-relay-listener"
		private const val NOTIFICATION_POLL_MILLIS = 1_000
	}
}
