package aidocs.doc_relay.admin

import org.junit.jupiter.api.Test
import org.springframework.boot.actuate.endpoint.OperationType
import org.springframework.boot.actuate.endpoint.invoke.reflect.OperationMethod
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 최종 리뷰에서 발견된 버그의 회귀 테스트. `act()` 의 `token` 파라미터는 코틀린 레벨 기본값
 * (`= null` 또는 예전의 `= ""`) 이 있지만, 액추에이터는 `@WriteOperation` 메서드를
 * `ReflectiveOperationInvoker` 로 호출하고, 파라미터가 필수인지는 코틀린 기본값이 아니라
 * `Nullness.forParameter(parameter) != Nullness.NULLABLE` 로 판단한다 — 이건 코틀린-리플렉션
 * (`kotlin-reflect`, 이미 의존성에 있음) 을 통해 선언된 타입의 null 허용 여부를 본다.
 *
 * non-null 타입 `String` 은 기본값 유무와 무관하게 `Nullness.NON_NULL` 로 잡히므로, 예전
 * 시그니처(`token: String = ""`) 는 액추에이터 관점에서 token 을 필수로 취급했다 — 그 결과
 * 문서화된 요청 형태(`{"action": "REPUBLISH"}`, token 필드 없음)가 `MissingParametersException`
 * 으로 400 을 받았다. `token: String?` 로 바꾸면 `Nullness.NULLABLE` 이 되어 선택 파라미터로
 * 잡힌다.
 *
 * 이 테스트는 직접 `act(...)` 를 코틀린 호출부에서 부르는 대신, 액추에이터가 실제로 쓰는 것과
 * 같은 `OperationMethod`/`OperationParameters` API 로 파라미터 필수 여부를 확인한다 — 그래야
 * "코틀린 호출부만 통과하고 실제 액추에이터 계약은 깨져 있는" 상태를 잡아낼 수 있다
 * (`OutboxEndpointTest`/`OutboxEndpointAuthTest` 는 코틀린에서 직접 `act(...)` 를 부르므로
 * 이 버그를 잡지 못했다).
 */
class OutboxEndpointActuatorContractTest {

	@Test
	fun `token is optional under Actuator's mandatoriness contract while id and action stay mandatory`() {
		val method = OutboxEndpoint::class.java.getDeclaredMethod(
			"act", String::class.java, String::class.java, String::class.java,
		)

		val parameters = OperationMethod(method, OperationType.WRITE).parameters

		assertTrue(parameters.get(0).isMandatory, "id 는 필수로 남아 있어야 한다")
		assertTrue(parameters.get(1).isMandatory, "action 은 필수로 남아 있어야 한다")
		assertFalse(
			parameters.get(2).isMandatory,
			"token 은 선택이어야 한다 — 그렇지 않으면 실제 액추에이터 호출기가 token 없이 보낸 " +
				"POST 요청을 MissingParametersException(400) 으로 거부한다",
		)
	}
}
