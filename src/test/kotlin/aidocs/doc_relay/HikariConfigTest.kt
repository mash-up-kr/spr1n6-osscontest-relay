package aidocs.doc_relay

import aidocs.doc_relay.support.RelayIntegrationTest
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import javax.sql.DataSource
import kotlin.test.assertEquals

class HikariConfigTest : RelayIntegrationTest() {

	@Autowired
	private lateinit var dataSource: DataSource

	@Test
	fun `hikari pool applies the configured timeouts`() {
		val hikari = dataSource as HikariDataSource

		assertEquals(5_000, hikari.connectionTimeout)
		assertEquals(3_000, hikari.validationTimeout)
		assertEquals(30_000, hikari.keepaliveTime)
		assertEquals(600_000, hikari.maxLifetime)
	}
}
