package aidocs.doc_relay

import aidocs.doc_relay.support.RelayIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Controller
import kotlin.test.assertTrue

/**
 * 이 서버는 요청을 받지 않는다. HTTP 표면은 Actuator 엔드포인트뿐이다.
 * @RestController 는 @Controller 로 메타 애너테이션되어 있어 둘 다 여기 걸린다.
 *
 * spring-boot-starter-web 이 클래스패스에 있으면 ErrorMvcAutoConfiguration 이
 * basicErrorController 를 자동 등록한다. 이건 애플리케이션이 만든 컨트롤러가 아니라
 * 프레임워크 인프라이므로 대상에서 제외한다 — 빈 이름(예: "basicErrorController")은
 * 짧은 관례적 이름이라 걸러지지 않으므로, 빈을 구현하는 클래스의 패키지로 판별한다.
 */
class NoControllersTest : RelayIntegrationTest() {

	@Autowired
	private lateinit var context: ApplicationContext

	@Test
	fun `no controller beans exist`() {
		val controllers = context.getBeansWithAnnotation(Controller::class.java)
			.filterValues { !it.javaClass.name.startsWith("org.springframework") }
		assertTrue(
			controllers.isEmpty(),
			"컨트롤러를 만들지 않는다 (spec §3). 발견된 빈: ${controllers.keys}",
		)
	}
}
