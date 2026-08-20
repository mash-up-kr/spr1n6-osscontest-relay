package aidocs.doc_relay.outbox

/**
 * 같은 에러 메시지를 공유하는 실패 묶음의 키. [permanent] 가 true 면 재시도해도 소용없는
 * 실패라 즉시 DEAD 로 보낸다.
 */
data class FailureGroup(val message: String, val permanent: Boolean)
