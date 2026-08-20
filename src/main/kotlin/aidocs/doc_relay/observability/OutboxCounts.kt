package aidocs.doc_relay.observability

/**
 * 상태별 건수. [pending] 은 발행을 기다리는 것, [publishing] 은 지금 발행 중인 것,
 * [dead] 는 포기했지만 자동으로 되살아날 것, [held] 는 사람이 멈춰 둔 것이다.
 *
 * [held] 를 따로 세는 이유는 멈춰 둔 행도 상태값은 여전히 DEAD 라서, 합쳐 세면 저절로
 * 회복될 것과 사람이 손대야 할 것이 구분되지 않기 때문이다. 멈춰 놓고 잊는 일이 잦다.
 */
data class OutboxCounts(
	val pending: Int,
	val publishing: Int,
	val dead: Int,
	val held: Int,
)
