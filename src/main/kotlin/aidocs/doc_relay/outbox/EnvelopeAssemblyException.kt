package aidocs.doc_relay.outbox

/**
 * 봉투 조립(payload 파싱 등) 실패를 감싼다. Kafka 클라이언트가 던지는 예외와 구분해
 * [FailureClassifier] 가 "다시 보내도 소용없는 실패"로 식별할 수 있게 하는 마커다.
 */
class EnvelopeAssemblyException(cause: Throwable) : RuntimeException(cause.message, cause)
