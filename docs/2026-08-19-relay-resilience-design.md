# 릴레이 서버 설계

`outbox_event` 테이블을 읽어 Kafka로 발행하는 독립 Spring Boot 서버를 설계한다. Tmax OpenSQL(PostgreSQL 17.8 기반) 위의 AI 문서 관리 플랫폼에서 Transactional Outbox 패턴의 발행자 역할을 맡는다.

---

## 1. 무엇을 푸는가

### 1.1 문제

사용자가 문서를 올리면 두 가지가 일어나야 한다.

1. 문서 정보를 DB에 저장한다
2. 임베딩 워커에게 "이 문서를 인덱싱하라"를 Kafka로 보낸다

DB와 Kafka는 서로 다른 시스템이라 하나의 트랜잭션으로 묶이지 않는다. 그래서 둘 중 하나만 성공하는 경우가 생긴다.

| 벌어지는 일 | 결과 |
| --- | --- |
| DB 저장 성공 → Kafka 발행 실패 | 문서는 있는데 영원히 인덱싱되지 않는다. 사용자는 올렸는데 검색이 안 된다 |
| Kafka 발행 성공 → DB 트랜잭션 롤백 | 존재하지 않는 문서를 인덱싱하려 든다 |

둘 다 조용히 일어난다. 아무도 모른다.

### 1.2 아웃박스 패턴

Kafka로 바로 보내지 않는다. 문서를 저장하는 바로 그 트랜잭션 안에서, 같은 DB의 `outbox_event` 테이블에 "보낼 것"을 한 줄 적는다. 문서 저장과 이벤트 기록이 하나의 트랜잭션이므로 둘 다 되거나 둘 다 안 된다. 위 두 경우가 원천적으로 사라진다.

이 프로젝트에서는 API 서버가 코드로 적지 않고 DB 트리거가 대신한다. `document_version` 에 INSERT가 들어오면 트리거가 같은 트랜잭션에서 `outbox_event` 한 줄을 만들고 `pg_notify` 로 알림을 쏜다. 트리거는 파트너 저장소 소유다.

그 줄을 읽어 Kafka로 보내는 것이 이 서버다.

### 1.3 전체 구조

```
  사용자 업로드
      │
      ▼
  API 서버 ──── 같은 트랜잭션 ────→ document_version
                                          │ DB 트리거
                                          ▼
                                    outbox_event  ← "보낼 것" 한 줄
                                          │ pg_notify
                                          ▼
                                    ┌───────────┐
                                    │  릴레이    │  ← 이 서버
                                    └───────────┘
                                          │
                                          ▼
                                      Kafka  doc.events.v1
                                          │
                                          ▼
                                     임베딩 워커
```

Debezium은 쓰지 않는다. 이 서버가 그 역할을 직접 한다.

### 1.4 이 서버가 하지 않는 일

경계를 좁게 유지하는 것이 이 설계의 전제다.

| 항목 | 담당 |
| --- | --- |
| 워커가 멈춘 Job 회수 | 임베딩 워커 (Kafka 오프셋 재전달) |
| 재인덱싱 | API 서버 (새 outbox 행 INSERT) |
| 순서 역전 판정 / 펜싱 | 임베딩 워커 |
| 중복 처리 흡수 | 임베딩 워커 (`source_event_id` 유니크 제약) |

그리고 다음 두 가지를 지킨다.

**릴레이는 `outbox_event` 에 INSERT도 DELETE도 하지 않는다.** 모든 쓰기가 `UPDATE ... WHERE id = ...` 다. 어드민 재발행조차 새 행을 만들지 않고 기존 행을 되돌린다. 새 행을 만드는 재발행은 재인덱싱의 몫이고 그 주체는 API 서버다.

**다른 팀의 테이블을 읽지 않는다.** `indexing_job` 을 비롯한 나머지 테이블에 접근하지 않는다. 남의 테이블의 의미를 알아야 하는 판정은 그 테이블을 소유한 쪽이 한다.

### 1.5 이 서버는 요청을 받지 않는다

HTTP 엔드포인트를 제공하지 않는다. 메인 포트를 닫고(`server.port: -1`) 관리 포트만 `127.0.0.1` 에 연다. 어드민 조작도 `@RestController` 가 아니라 Actuator 엔드포인트로 만든다.

Actuator의 HTTP 노출이 서블릿 컨테이너 위에 얹히므로 `spring-boot-starter-web` 의존성 자체는 들어온다. 요청을 받지 않는다는 보장은 의존성 부재가 아니라 위 두 가지와, 컨트롤러 빈이 하나라도 있으면 실패하는 테스트로 강제한다.

---

## 2. 데이터 모델

### 2.1 릴레이가 기대하는 형태

```sql
CREATE TABLE outbox_event (
    id                      UUID PRIMARY KEY,
    tenant_id               BIGINT      NOT NULL,
    document_id             BIGINT      NOT NULL,
    document_version_id     BIGINT      NOT NULL,
    event_type              VARCHAR(50) NOT NULL,
    event_schema_version    INTEGER     NOT NULL DEFAULT 1,
    payload                 JSONB       NOT NULL,   -- 트리거 생성. 그대로 통과
    trace_id                VARCHAR(255),
    status                  VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    publish_attempt_count   INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_by               VARCHAR(255),
    locked_at               TIMESTAMPTZ,
    published_at            TIMESTAMPTZ,
    last_error_message      TEXT,
    retry_of_event_id       UUID,                   -- API 서버가 재인덱싱 시 기록
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_outbox_status
        CHECK (status IN ('PENDING','PUBLISHING','PUBLISHED','DEAD'))
);

CREATE INDEX idx_outbox_pending ON outbox_event (next_attempt_at, id) WHERE status = 'PENDING';
CREATE INDEX idx_outbox_stuck   ON outbox_event (locked_at)           WHERE status = 'PUBLISHING';
```

`idx_outbox_pending` 의 컬럼 순서가 선점 쿼리의 `ORDER BY` 와 일치해야 한다. 일치하지 않으면 매 사이클 정렬이 붙는다.

### 2.2 릴레이가 쓰는 컬럼

| 컬럼 | 권한 |
| --- | --- |
| `status` · `locked_by` · `locked_at` · `next_attempt_at` · `publish_attempt_count` · `published_at` · `last_error_message` | UPDATE |
| `payload` · `trace_id` · 나머지 | 읽기만 |

운영 스키마는 파트너 저장소가 소유한다. 이 저장소는 Flyway를 포함하지 않고, Testcontainers용 픽스처 SQL만 둔다. 픽스처와 운영 마이그레이션의 동기화 책임은 이쪽에 있다.

### 2.3 통과만 시키는 두 컬럼

`event_schema_version` 과 `trace_id` 는 릴레이가 의미를 해석하지 않고 봉투와 헤더로 옮기기만 한다. 봉투 조립의 "payload 를 파싱하지 않는다"와 같은 이유로 존재한다 — 릴레이가 이벤트의 내용을 몰라도 되게 한다.

**`event_schema_version`** 은 `payload` 의 형태가 몇 번째 판인지를 가리킨다. 트리거가 항상 `1` 로 박아 넣고, 릴레이는 그 값을 봉투의 `schemaVersion` 필드와 Kafka 헤더로 복사한다. 지금은 판이 하나뿐이라 이 값으로 분기하는 코드가 어디에도 없다. 나중에 `payload` 의 필드 구성이 호환되지 않게 바뀔 때, 받는 쪽이 옛 판과 새 판을 갈라 파싱할 수 있게 미리 자리를 잡아 둔 것이다. 릴레이는 그때도 바뀌지 않는다.

**`trace_id`** 는 이 이벤트를 만들어 낸 원래 요청의 추적 식별자다. 트리거가 `current_setting('app.trace_id', true)` 로 읽어 채운다 — API 서버가 `document_version` 을 INSERT 하기 전에 같은 트랜잭션에서 `SET app.trace_id` 를 해 두었으면 그 값이 실려 오고, 안 했으면 `NULL` 이다. 릴레이는 이 값을 Kafka 헤더와 로그 MDC 에 얹는다.

`trace_id` 가 필요한 이유는 이벤트가 DB 트리거로 태어나기 때문이다. 업로드 요청과 Kafka 메시지 사이에 트랜잭션 경계와 비동기 발행이 끼어 있어서, 이 값이 없으면 "이 메시지가 어느 사용자 요청에서 시작됐는지"를 이어 볼 방법이 없다. 릴레이는 이 값을 만들지도 고치지도 않고 옮기기만 한다.

### 2.4 기동 시 스키마 검증

`information_schema` / `pg_catalog` 조회로 확인한다.

| 검증 | 불일치 시 |
| --- | --- |
| 필수 컬럼 존재 (특히 `next_attempt_at`) | 기동 중단 |
| `next_attempt_at` 이 NOT NULL 인가 | 기동 중단 |
| `ck_outbox_status` 가 `DEAD` 를 허용하는가 | 기동 중단 |
| 인덱스 두 개 존재 | WARN 로그 |

컬럼과 CHECK는 없으면 동작이 깨지므로 즉시 죽인다. `next_attempt_at` 이 nullable인 DB에 붙으면 선점 쿼리가 조용히 0건을 반환하고, 릴레이는 "발행할 게 없다"고 판단해 정상처럼 보인다. 이벤트는 계속 쌓이는데 아무도 모르는 상태가 되므로, 실패는 시끄러운 쪽이 낫다.

인덱스는 없어도 결과가 맞으므로 경고만 남긴다.

---

## 3. 구성 요소

```
릴레이/
├─ 신호 경로
│   ├─ PgNotificationListener   전용 커넥션으로 LISTEN. 재연결 + 재연결 후 신호 1회
│   ├─ PollingScheduler         10초 안전망
│   └─ DrainTrigger             신호 합치기 + 드레인 스레드 1개
│
├─ 발행
│   ├─ OutboxDrainer            사이클: 선점 → 발행 → 반영 → 재사이클 판단
│   ├─ OutboxRepository         모든 SQL. 선점 / 반영 / 회수 / 복구 / 집계
│   ├─ EnvelopeAssembler        행 → 봉투 (payload 무파싱 통과)
│   ├─ KafkaPublisher           배치 send + flush + 성공/실패 분류
│   ├─ KafkaProducerConfig      acks=all, 멱등 프로듀서
│   ├─ FailureClassifier        영구 실패 / 일시 실패 판정   미구현
│   └─ BackoffPolicy            min(base × 2^(n-1), max)
│
├─ 복구
│   ├─ ZombieRecoveryScheduler  PUBLISHING + 락 만료 → PENDING
│   └─ DeadRecoveryScheduler    DEAD + 시간 도래 → PENDING
│
├─ 수명주기
│   ├─ SchemaValidator          스키마 검증
│   ├─ BudgetValidator          타임아웃 예산 검증           미구현
│   └─ RelayHealthIndicator     드레인 스레드 · DB 상태      미구현
│
└─ 관측 / 조작
    ├─ RelayMetrics             Micrometer + 게이지 갱신
    └─ OutboxEndpoint           조회 / 재발행 / 정지 / 해제
```

SQL은 `OutboxRepository` 한 곳에만 둔다. 다른 클래스는 SQL 문자열을 갖지 않는다.

DB 접근은 JdbcClient를 쓴다. `SKIP LOCKED` 와 부분 인덱스를 타는 쿼리가 이 서버의 핵심이라 SQL을 직접 제어한다. JPA 엔티티는 두지 않는다. 시각 타입은 `Instant` 로 통일한다 — 모든 시각 컬럼이 `TIMESTAMPTZ` 이므로 `LocalDateTime` 을 쓰지 않는다.

모든 타임아웃·주기·상한은 프로퍼티로 뺀다. 코드에 숫자를 박지 않는다.

---

## 4. 신호 경로

새 이벤트가 생겼다는 것을 아는 방법이 셋이고, 전부 한 곳으로 모인다.

```
  PgNotificationListener ─┐   트리거의 pg_notify 를 받고
  PollingScheduler ───────┼─→ 주기가 돌아왔고        ─→ DrainTrigger.signal() ─→ 드레인 스레드 1개
  OutboxEndpoint ─────────┘   사람이 REPUBLISH 를 눌렀고
```

**셋 다 "깨워라"만 보낸다.** 무엇을 발행할지는 셋 중 누구도 정하지 않는다. 어느 경로로 깨어났든 드레인 사이클은 똑같이 DB에 `PENDING` 이고 시간이 된 행을 다시 묻는다. 그래서 경로를 하나 더 붙이거나 하나가 죽어도 발행 로직은 바뀌지 않는다.

`OutboxEndpoint` 가 여기 끼는 것은 `REPUBLISH` 때문이다. 어드민이 `DEAD` 행을 `PENDING` 으로 되돌려 놓고 신호를 쏘지 않으면, 사람이 "지금 다시 보내라"를 눌렀는데 다음 폴링 주기까지 아무 일도 일어나지 않는다. 되돌리는 UPDATE와 신호가 짝이다.

### 4.1 DB 알림

커넥션 풀이 아니라 전용 커넥션 하나를 따로 열어 `LISTEN outbox_event` 를 건다. 트리거가 `pg_notify` 를 쏘면 즉시 깨어난다. 업로드 후 1초 안에 Kafka에 도착하는 것이 이 경로 덕분이다.

두 가지를 지킨다.

**알림에 실려 온 이벤트 UUID를 조회 키로 쓰지 않는다.** 신호는 드레인 사이클을 깨우기만 하고, 무엇을 보낼지는 항상 DB에 다시 묻는다. 알림을 진실의 원천으로 삼으면 유실된 알림의 이벤트는 영원히 발행되지 않는다.

**재연결 직후 무조건 신호를 한 번 쏜다.** 끊긴 동안 유실된 알림을 이 한 번이 전부 보상한다.

페일오버 시 제일 먼저 끊어지는 지점이므로 재연결 경로는 통합 테스트로 직접 증명한다.

### 4.2 폴링

10초 고정 주기로 신호를 쏜다. 알림 유실과 리스너 다운을 동시에 덮는 안전망이다. 리스너가 완전히 죽어도 릴레이는 10초 지연으로 계속 동작한다.

스케줄러 자신은 DB를 조회하지 않는다. `next_attempt_at` 이 지난 행을 찾는 것도, 발행할 게 있는지 판단하는 것도 전부 드레인 사이클의 선점 쿼리가 한다. 폴링이 하는 일은 `signal()` 한 줄이 전부이고, 그래서 DB가 느리거나 죽어 있어도 이 경로 자체는 막히지 않는다.

### 4.3 신호 합치기

`DrainTrigger` 는 "다음 사이클이 필요하다"는 사실 하나만 들고 있다. 용량 1짜리 큐가 그 사실을 담으므로 신호가 1000개 와도 추가 드레인은 최대 1회다.

이것이 성립하는 이유는 큐에 넣는 연산이 자리가 없으면 그냥 버리는 쪽이기 때문이다. 큐가 이미 차 있으면 새 신호는 조용히 사라진다. 담긴 것과 버려진 것이 어차피 같은 사실("깨워라")이라 잃는 정보가 없다.

**신호 1회와 이벤트 1건은 대응하지 않는다.** 이벤트 1000건이 한꺼번에 쌓여도 드레인 스레드는 한 번 깨어나고, 대신 사이클 안에서 백로그가 빌 때까지 반복한다. 깨어나는 횟수와 처리하는 건수를 분리해 둔 것이 신호를 접을 수 있는 근거다.

드레인 사이클은 한 번에 하나만 돈다. 이 서버의 동시성 지점은 여기 하나뿐이다.

드레인 스레드는 생성자가 아니라 `SmartLifecycle.start()` 에서 띄운다. 생성자 안에서 스레드를 띄우면 빈이 완전히 초기화되기 전에 그 스레드가 인스턴스를 참조할 수 있다.

---

## 5. 드레인 파이프라인

한 사이클은 세 단계다. 네트워크 IO는 트랜잭션 밖에서만 일어난다.

```
 ┌──────────────────────────────────────────────────────┐
 │ ① 선점   DB 한 문장 (CTE + RETURNING)                │  IO 없음, 짧다
 │          PENDING 이고 시간 된 것을 batchSize 만큼      │
 │          PUBLISHING 으로 바꾸고 내용을 가져온다        │
 ├──────────────────────────────────────────────────────┤
 │ ② 발행   Kafka로 전부 보내고 flush() 한 번으로 수집    │  트랜잭션 밖
 ├──────────────────────────────────────────────────────┤
 │ ③ 반영   성공 → PUBLISHED                            │  다시 DB
 │          실패 → PENDING + 백오프  또는  DEAD          │
 └──────────────────────────────────────────────────────┘
        │
        └→ 선점 건수 == batchSize 이면 백로그가 남았다는 뜻 → 즉시 다시 ①
```

세 단계로 나눈 이유가 이 설계의 핵심이다. **행에 락을 건 채로 Kafka를 기다리면, Kafka가 느려질 때 DB 락이 그만큼 오래 잡힌다.** 선점을 짧은 트랜잭션 하나로 끝내고, 락을 놓은 상태에서 네트워크로 나간다.

### 5.1 선점

```sql
WITH claimed AS (
    SELECT id FROM outbox_event
     WHERE status = 'PENDING' AND next_attempt_at <= now()
     ORDER BY next_attempt_at, id
     LIMIT :limit
       FOR UPDATE SKIP LOCKED
)
UPDATE outbox_event o
   SET status = 'PUBLISHING', locked_by = :instanceId, locked_at = now()
  FROM claimed c WHERE o.id = c.id
RETURNING o.id, o.tenant_id, o.document_id, o.document_version_id,
          o.event_type, o.event_schema_version, o.payload::text,
          o.trace_id, o.publish_attempt_count, o.created_at,
          o.locked_at;
```

애플리케이션은 행을 하나씩 읽어 보며 고르지 않는다. **고르기 · 표시하기 · 가져오기가 전부 이 한 문장 안에서 끝난다.** 옵티마이저가 부분 인덱스로 조건에 맞는 행을 `batchSize` 만큼 집어 잠그고, 같은 문장이 그 행들을 `PUBLISHING` 으로 바꾸고, `RETURNING` 이 바뀐 내용을 그대로 돌려준다. DB 왕복은 1회다.

세 가지가 한 문장에 들어 있다.

- `FOR UPDATE SKIP LOCKED` — 다른 인스턴스가 잡은 행은 건너뛴다. 릴레이를 여러 대 띄워도 코드를 바꿀 필요가 없다.
- `ORDER BY next_attempt_at, id` — `idx_outbox_pending` 의 컬럼 순서와 같아서 정렬이 붙지 않고, `WHERE` · `ORDER BY` · `LIMIT` 를 인덱스 하나가 받는다.
- CTE 한 문장 — SELECT와 UPDATE를 나누면 그 사이에 락을 놓치거나, 반대로 락을 쥔 채 밖으로 나가는 문이 열린다.

`RETURNING` 의 `locked_at` 은 결과 쓰기 소유권 확인에 쓰인다. **미구현** — 현재 선점 SQL은 이 컬럼을 돌려주지 않는다.

### 5.2 발행

배치 전체를 먼저 밀어 넣고, `flush()` 한 번으로 응답을 받는다. 한 건 보내고 응답을 기다리기를 반복하지 않는다.

`send()` 는 네트워크로 나가지 않는다. 프로듀서 내부 버퍼에 메시지를 쌓고 `Future` 하나를 돌려주고 즉시 끝난다. `flush()` 가 그제서야 쌓인 것을 파티션별로 묶어 브로커로 보내고, 모든 응답이 돌아올 때까지 블로킹한다. 브로커의 응답 하나에 여러 메시지의 결과가 함께 실려 오므로, 100건을 보내도 네트워크 왕복은 100번이 아니다.

**왕복을 줄인 것이지 확인을 건너뛴 것이 아니다.** `flush()` 가 끝난 뒤 행마다 `Future` 를 열어 성공과 실패를 가른다. 이미 결론이 난 상태라 이 단계는 기다리지 않는다. 배치 안에서 일부만 실패하는 것이 정상이고, 그 결과를 성공 목록과 에러 메시지별 실패 묶음으로 나눠 반영 단계에 넘긴다.

```
Topic     doc.events.v1        (partitions = 3)
Key       document_id (String)  ← 문서 단위 순서 보장. 변경 금지
Headers   eventId / traceId / schemaVersion
Producer  acks=all, enable.idempotence=true
전달 보장  at-least-once
```

Kafka 트랜잭션은 쓰지 않는다. 멱등 프로듀서만 켠다.

한 행의 봉투 조립이 실패해도 나머지 행은 계속 보낸다. 조립은 행 단위로 감싸서 실패한 행만 실패 목록으로 보내고, 나머지는 그대로 발행한다.

### 5.3 봉투 조립

```json
{
  "eventId": "...",
  "tenantId": 1,
  "documentId": 42,
  "documentVersionId": 137,
  "eventType": "INDEXING_REQUESTED",
  "schemaVersion": 1,
  "occurredAt": "2026-08-13T09:14:22Z",
  "payload": { }
}
```

세 원칙을 지킨다.

**`payload` 를 파싱하지 않는다.** JSONB 문자열을 JSON 노드로 그대로 꽂는다. 릴레이가 payload 스키마를 몰라도 되고, 파트너가 필드를 추가해도 릴레이는 바뀌지 않는다.

**`eventType` 은 컬럼 값을 복사하고 분기하지 않는다.** 새 이벤트 종류가 CHECK에 추가되는 날 릴레이는 손댈 것이 없다.

**최상위 필드는 컬럼에서 복원한다.** payload 안의 값을 끌어올리지 않는다. 최상위 `occurredAt` 은 `created_at` 에서 온다.

봉투 형태 자체는 그룹2와 확정 대기 중이다. `payload` 블록을 없애고 식별자만 평평하게 싣는 방향이 나왔고, 확정되면 `EnvelopeAssembler` 와 그 테스트만 고친다.

### 5.4 결과 반영

성공한 건은 한 문장으로 내린다. 실패한 건은 **에러 메시지를 키로 묶어** 묶음마다 UPDATE 한 번씩 날린다.

묶는 단위가 메시지인 이유는, 같은 메시지를 쓸 행들끼리는 `last_error_message` 가 같아서 한 문장에 들어가기 때문이다. 원인이 제각각이면 묶음이 여러 개 생기고 그만큼 UPDATE도 늘어난다. 반대로 Kafka 브로커가 통째로 죽으면 배치 100건이 전부 같은 예외로 실패하므로 묶음이 하나뿐이고, `WHERE id IN (:ids)` 한 문장이 100건을 끝낸다. 최악의 장애가 오히려 가장 싸게 처리되는 셈이다.

정상 경로의 DB 왕복은 사이클당 3회다 — 선점 1회, 성공 반영 1회, 실패 반영 1회.

백오프는 애플리케이션이 아니라 SQL 식으로 계산한다. 행마다 `publish_attempt_count` 가 다르므로 애플리케이션에서 계산한 단일 값을 넘기면 배치 UPDATE로 접을 수 없다.

```sql
next_attempt_at = now() + LEAST(:baseSeconds * POWER(2, publish_attempt_count), :maxSeconds)
                          * INTERVAL '1 second'
```

### 5.5 중복 발행이 발생하는 지점

②에서 Kafka ack를 받고 ③을 커밋하기 전에 프로세스가 죽으면, 그 행은 `PUBLISHING` 으로 남았다가 좀비 회수를 거쳐 다시 발행된다.

이것은 at-least-once 계약상 정상이며 워커 멱등성(`source_event_id` 유니크, 청크 UPSERT)이 흡수한다. 없애려면 Kafka 트랜잭션과 DB 트랜잭션을 묶어야 하는데 그럴 가치가 없다. exactly-once를 추구하지 않는다.

이 계약은 테스트로 못 박는다. 버그를 잡는 테스트가 아니라 계약을 문서화하는 테스트다. 나중에 "왜 중복이 오냐"가 나올 때 그 테스트가 답이 된다.

---

## 6. 실패 처리

### 6.1 상태 전이

```
   트리거 INSERT
         │
         ▼
    ┌─────────┐    선점     ┌────────────┐   ack   ┌───────────┐
    │ PENDING │ ──────────→ │ PUBLISHING │ ──────→ │ PUBLISHED │
    └─────────┘             └────────────┘         └───────────┘
         ▲                        │
         │  일시 실패 (5회 미만)   │
         ├────────────────────────┤
         │  락 만료 (좀비 회수)    │
         ├────────────────────────┘
         │
         │                     일시 실패 5회째
         │                     ┌──────────┐
         │  복구 지연 후 자동   │          │
         └─────────────────────│   DEAD   │
              또는 어드민 조작  │          │
                               └──────────┘
                                    ▲  │
                    영구 실패 ───────┘  │ 정지(HOLD)
                    (즉시, 재시도 없이)  ▼
                     미구현       next_attempt_at = 'infinity'
                                → 자동 복구에서 빠진다
```

### 6.2 영구 실패와 일시 실패를 나눈다   미구현

발행 실패를 전부 똑같이 다루면, 다시 해도 소용없는 실패에 재시도 5회를 낭비하고, 그 뒤 자동 복구를 타고 영원히 순환한다.

`FailureClassifier` 가 예외를 보고 판정한다.

| 분류 | 무엇이 | 처리 |
| --- | --- | --- |
| 영구 | 봉투 조립 실패 | 즉시 `DEAD` + `next_attempt_at = 'infinity'` |
| 영구 | `RecordTooLargeException` | 〃 |
| 영구 | `TopicAuthorizationException` | 〃 |
| 영구 | `InvalidTopicException` | 〃 |
| 일시 | 그 외 전부 | 백오프 후 재시도 |

영구 실패는 곧바로 "정지된 `DEAD`" 가 된다. 새 상태도 새 테이블도 스키마 변경도 필요 없다 — 정지 스위치를 그대로 재사용한다. 결과적으로 재시도를 낭비하지 않고, 순환하지 않고, `relay.outbox.held` 게이지에 바로 잡히고, 사람이 원인을 고친 뒤 `RELEASE` 로 되살린다.

**분류에 확신이 없으면 일시로 둔다.** 잘못 재시도하면 낭비지만, 잘못 영구로 판정하면 멈춰선 안 될 이벤트가 멈춘다.

`last_error_message` 앞에 분류를 붙여 어드민 목록에서 바로 보이게 한다.

### 6.3 백오프

`n` 번째 실패 후 대기는 `min(base × 2^(n-1), max)` 다. 기본값(`base=10s`, `max=5m`, `maxAttempts=5`)에서 이렇게 흐른다.

| 실패 | `publish_attempt_count` | 다음 시도까지 |
| --- | --- | --- |
| 1회 | 1 | 10초 |
| 2회 | 2 | 20초 |
| 3회 | 3 | 40초 |
| 4회 | 4 | 80초 |
| 5회 | 5 | `count + 1 >= maxAttempts` → `DEAD` |

표의 10·20·40·80초는 `DEAD` 에 닿기 전까지의 짧은 재시도 간격이다. 여기에 자동 복구가 붙으면 이벤트 하나가 도는 큰 주기는 따로 생긴다. Kafka가 계속 죽어 있다고 가정하면 이렇게 흐른다.

```
0초     1차 실패
150초   5차 실패 → DEAD.  next_attempt_at = now + 10분    (10+20+40+80 = 2분 30초)
750초   복구 지연 끝 → PENDING, publish_attempt_count = 0
750초   다시 1차 실패 → 위를 처음부터 반복
```

**재시도 간격이 12분 30초가 아니라, "실패 → `DEAD` → 복구 → 다시 실패" 한 바퀴가 12분 30초다.** 2분 30초는 재시도에 쓰고 나머지 10분은 `DEAD` 로 가만히 있는다.

`max`(5분)가 실제로 걸리려면 대기가 300초를 넘겨야 하는데, `10 × 2^(n-1)` 이 처음 넘는 것은 6번째(320초)다. `maxAttempts=5` 에서는 5번째 실패가 대기를 계산하기도 전에 `DEAD` 로 빠지므로 6번째가 오지 않는다. 6번째 실패까지 재시도가 이어지려면 `maxAttempts` 가 7 이상이어야 하고, 그때부터 `max` 가 의미를 갖는다.

시연에서는 이 주기가 너무 길어 `demo` 프로파일에서 값을 낮춘다.

### 6.4 `DEAD` 는 종착역이 아니다

`DEAD` 로 간 행을 복구 지연(기본 10분) 뒤 자동으로 `PENDING` 으로 되돌린다.

```sql
UPDATE outbox_event
   SET status = 'PENDING', publish_attempt_count = 0, next_attempt_at = now()
 WHERE status = 'DEAD' AND next_attempt_at <= now();
```

Kafka가 20분 죽어 있었다면 그건 이벤트 잘못이 아니다. 사람 손을 빌리지 않고 회복되는 쪽이 맞다. DLQ 토픽은 만들지 않는다.

`DEAD` 전환 시 `next_attempt_at = now() + recoveryDelay` 를 찍어 두므로 방금 죽은 행은 잡히지 않는다. `updated_at` 컬럼 없이 해결된다.

**대가는 종착 상태가 없다는 것이다.** 발행이 구조적으로 불가능한 행은 영원히 순환한다. 실패 분류가 자동으로 구분할 수 있는 것들을 걸러내지만, 걸러지지 않는 경우를 위해 사람이 멈출 수단을 둔다.

- 정지(HOLD) — `next_attempt_at` 을 `'infinity'` 로 민다. 위 쿼리의 `next_attempt_at <= now()` 에서 자연스럽게 빠진다. 스키마 변경 없이 정지·재개가 된다.
- 해제(RELEASE) — `now()` 로 되돌린다.

정지된 행은 `status` 가 여전히 `DEAD` 이고 `next_attempt_at` 만 다르므로 눈에 잘 띄지 않는다. 사람이 멈춰 놓고 잊는 일이 실제로 잦으므로, 게이지(`relay.outbox.held`)와 조회 응답의 `held` 플래그 양쪽에 드러낸다.

순환 횟수는 DB에 남지 않는다 — 자동 복구가 `publish_attempt_count` 를 0으로 리셋하기 때문이다. `DEAD` 전환마다 WARN 로그와 카운터를 남기며, 그 카운터가 유일한 관측 창이다.

### 6.5 발행 시간 예산   미구현

지켜야 할 관계를 문장으로 먼저 정한다.

> **한 사이클의 최대 소요 시간 < 좀비 회수 타임아웃**

이게 깨지면 회수가 아직 발행 중인 배치를 뺏고, 그 순간 소유권 확인이 막으려는 상황이 실제로 발생한다. 둘은 짝이다.

프로듀서 타임아웃을 기본값에 맡기지 않고 명시한다.

```
max.block.ms         10초    메타데이터 대기 상한
request.timeout.ms   30초    요청 1회 상한
delivery.timeout.ms  120초   재시도 포함 전체 상한
max.request.size     1MB     메시지 크기 상한
```

| 항목 | 값 |
| --- | --- |
| Kafka 발행 상한 | 130초 (`max.block.ms` + `delivery.timeout.ms`) |
| DB 왕복 여유 | 30초 (커넥션 타임아웃이 보장) |
| **한 사이클 상한** | **160초** |
| **좀비 회수 타임아웃** | **300초** |
| 여유 | 140초 |

설정을 잘못 바꾸면 조용히 깨지므로 `BudgetValidator` 가 기동 시 이 관계를 검사하고, 어긋나면 스키마 검증과 같은 방식으로 죽인다.

`demo` 프로파일은 좀비 타임아웃을 30초로 줄이므로 프로듀서 타임아웃도 함께 줄여야 한다. 기동 검사가 이걸 잡는다.

---

## 7. 복구 경로

### 7.1 좀비 회수

`PUBLISHING` 인 채로 락이 만료된 행을 `PENDING` 으로 되돌린다. 1분 주기, UPDATE 한 방이다.

```sql
UPDATE outbox_event
   SET status = 'PENDING',
       publish_attempt_count = publish_attempt_count + 1,
       next_attempt_at = now() + LEAST(:baseSeconds * POWER(2, publish_attempt_count), :maxSeconds)
                                 * INTERVAL '1 second',
       last_error_message = 'reclaimed: publishing lock timeout',
       locked_by = NULL, locked_at = NULL
 WHERE status = 'PUBLISHING' AND locked_at < now() - :zombieLockTimeout;
```

`idx_outbox_stuck` 을 탄다. 회수는 여러 행을 한 번에 처리하고 행마다 카운트가 다르므로 백오프를 SQL 식으로 계산한다.

`locked_at < now() - :zombieLockTimeout` 은 타임아웃을 음수로 쓰는 것이 아니라, 현재 시각에서 그 기간만큼 뒤로 물린 기준선을 만드는 것이다. 타임아웃이 5분이고 지금이 `12:10` 이면 기준선은 `12:05` 이고, `locked_at` 이 그보다 이르면(= 5분 넘게 잠겨 있으면) 회수 대상이 된다.

**`next_attempt_at` 은 보지 않는다.** 그 컬럼은 행이 `PENDING` 일 때 "언제 다시 시도할지"를 뜻하고, 읽는 곳은 선점 쿼리 하나뿐이다. 회수가 다루는 행은 `PUBLISHING` 이라 선점 쿼리의 `WHERE status = 'PENDING'` 에서 이미 빠져 있고, 그동안 그 컬럼에 무엇이 적혀 있든 아무도 보지 않는다. 회수 시점에 필요한 질문은 "얼마나 오래 잠겨 있었나" 하나이고 그 답은 `locked_at` 에만 있다. 값은 어차피 이 UPDATE가 새로 계산해 덮어쓴다.

**카운트를 올린다.** 올리지 않으면 릴레이를 반복해서 죽이는 행이 회수 ↔ 재시도를 무한 반복하며 `DEAD` 에 영원히 도달하지 못한다.

**항상 `PENDING` 으로만 보낸다.** 카운트가 임계치를 넘었어도 여기서 `DEAD` 로 내리지 않는다. 회수 시점은 "락이 만료됐다"는 사실만 아는 시점이지 Kafka가 여전히 죽어 있다는 보장이 없다. 릴레이가 배포로 재시작됐거나 GC로 잠깐 멈춘 경우도 여기 걸리는데, 그걸 `DEAD` 로 내리면 멀쩡한 이벤트가 복구 지연만큼 대기한다. `DEAD` 판정은 실제 발행이 실패한 시점에만 한다.

### 7.2 결과 쓰기 소유권 확인   미구현

좀비 회수가 있기 때문에, **한 행의 소유권은 사이클 도중에 넘어갈 수 있다.** 그래서 결과를 쓸 때 그 행이 아직 내 것인지 확인해야 한다.

확인하지 않으면 이런 일이 벌어진다.

```
 릴레이 A 가 선점            (PUBLISHING, locked_at = T1)
      │  A 가 GC 또는 네트워크로 멈춤
      ▼
 좀비 회수가 락 만료로 보고 PENDING 으로 되돌림
      ▼
 릴레이 B 가 선점 → 발행 성공 → PUBLISHED
      ▼
 A 가 깨어나 예전 사이클의 실패를 기록
      ▼
 PUBLISHED 였던 행이 PENDING 으로 되돌아간다 → 다시 발행된다
```

반대 방향도 성립한다. A의 늦은 성공 기록이 B가 방금 잡아 발행 중인 행을 `PUBLISHED` 로 덮으면, B의 결과는 아무 데도 기록되지 않고 실패했더라도 재시도되지 않는다.

"중복은 계약상 정상"은 같은 이벤트가 두 번 나가는 경우를 가정한 방어다. 위는 결과 쓰기가 남의 행에 착지하는 경우라 성격이 다르다.

**스키마 변경 없이 막는다.** 소유권 표식용 컬럼을 새로 만들지 않고, 이미 있는 `locked_at` 을 "이번 사이클의 표식"으로 쓴다.

성립하는 근거는 PostgreSQL의 `now()` 가 실시간 시계가 아니라 트랜잭션 시각이라는 점이다. 선점은 CTE + UPDATE 한 문장이므로 그 안에서 100건을 동시에 갱신해도 `now()` 는 한 번만 평가되고, 따라서 **한 배치로 잡은 행들은 전부 같은 `locked_at` 을 갖는다.** 값 하나가 배치 전체를 가리키므로 `RETURNING` 으로 그것만 받아 두었다가 결과 쓸 때 조건에 넣으면, 행 단위로 쪼개지 않고 배치 UPDATE를 유지한 채 소유권 확인이 붙는다.

```sql
UPDATE outbox_event
   SET status = 'PUBLISHED', published_at = now(), locked_by = NULL, locked_at = NULL
 WHERE id IN (:ids)
   AND status    = 'PUBLISHING'
   AND locked_by = :instanceId
   AND locked_at = :claimedAt
```

실패 기록에도 같은 세 조건을 붙인다.

| 조건 | 막는 것 |
| --- | --- |
| `status = 'PUBLISHING'` | 이미 `PUBLISHED` 가 된 행을 되돌리는 것 |
| `locked_by = :instanceId` | 다른 인스턴스가 잡은 행에 쓰는 것 |
| `locked_at = :claimedAt` | 같은 인스턴스가 회수 후 다시 잡은 행에, 예전 사이클의 결과를 쓰는 것 |

두 번째와 세 번째가 막는 것이 다르다. `locked_by` 는 인스턴스 A와 B를 가르고, `locked_at` 은 같은 A 안에서 지금 사이클과 회수당한 예전 사이클을 가른다.

세 번째가 없으면 릴레이가 한 대일 때 막히지 않는다. A가 선점한 뒤 GC로 느려져 회수당하고, 살아 있던 A가 다음 사이클에서 같은 행을 다시 잡으면 `locked_by` 는 여전히 `A` 다. 인스턴스가 하나뿐인 배포에는 "다른 인스턴스"가 아예 없으므로 두 번째 조건은 항상 통과하고, 뒤늦게 도착한 예전 사이클의 결과를 걸러 낼 수 있는 것은 `locked_at` 뿐이다. 셋 다 필요하다.

갱신 행 수가 기대와 다르면 경고 로그와 `relay.stale.write.total` 카운터를 남긴다. 조건에 걸려 튕긴 쓰기가 곧 이 경합이 실재한다는 증거다.

> `now()` 가 한 문장 안에서 고정된다는 전제 위에 배치 UPDATE가 성립한다. 구현 시 이것부터 테스트로 확인한다.

### 7.3 결과 기록 실패 처리   미구현

성공 기록과 실패 기록을 서로 독립시킨다. 하나가 던져도 다른 하나는 돌고, 지표는 항상 갱신한다.

성공 기록을 먼저 하고 실패 기록을 나중에 하는 순차 구조에서, 앞이 예외를 던지면 뒤가 한 줄도 실행되지 않는다. 배치 전체가 `PUBLISHING` 으로 남아 좀비 회수를 기다리는 것까지는 안전망이 있어 괜찮지만, 지표가 같이 멈추면 관측 창이 닫힌다. DB가 흔들리는 상황에서 정확히 이게 일어난다.

기록에 실패한 사실 자체를 `relay.mark.failure.total` 로 센다.

---

## 8. 프로세스 수명주기

### 8.1 기동

1. `SchemaValidator` — 스키마 검증
2. `BudgetValidator` — 타임아웃 예산 검증   미구현
3. 드레인 스레드 · LISTEN 스레드 기동

검증이 어긋나면 기동을 중단한다. 조용히 오동작하는 것보다 안 뜨는 게 낫다.

### 8.2 종료   미구현

정상 종료(SIGTERM)가 강제 종료와 같은 뒤처리를 남기면 안 된다. 종료 때마다 진행 중이던 배치가 `PUBLISHING` 으로 남으면, 배포할 때마다 좀비 타임아웃 5분 + 스캔 주기 1분이 붙는다.

세 가지로 처리한다.

**사이클 안쪽에서도 종료 신호를 본다.** 백로그를 비우는 반복문이 매 사이클마다 종료 여부를 확인한다. 사이클과 사이클 사이에서만 확인하면 백로그가 클 때 종료 신호를 오래 무시한다.

**진행 중인 사이클이 끝날 때까지 기다린다.** 대기 상한(기본 30초)을 두고, 그 안에 안 끝나면 경고를 남기고 중단한다. 무한히 기다리면 배포가 멈춘다. 못 기다린 경우에는 좀비 회수가 안전망으로 남는다 — 없애는 게 아니라 평상시에 쓰이지 않게 만드는 것이다.

**종료 중에는 새 신호를 받지 않는다.**

결과적으로 배포는 "하던 배치를 끝내고 내려간다"가 되고, 좀비 회수는 진짜 크래시 때만 발동한다.

### 8.3 헬스체크   미구현

이 서버는 HTTP 요청을 받지 않으므로, 문제를 알아채는 경로가 헬스체크와 지표뿐이다.

| 대상 | 판정 |
| --- | --- |
| 드레인 스레드가 살아 있는가 | 죽었으면 `DOWN` |
| DB에 연결되는가 | 안 되면 `DOWN` |
| LISTEN 커넥션이 붙어 있는가 | `UP` 유지, 상세에만 표기 |

**드레인 스레드가 죽으면 `DOWN` 이다.** 재시작하면 낫는 문제이므로 liveness에 넣는다. 이걸 연결하지 않으면 릴레이가 살아 있는 채로 아무 일도 하지 않고, 적체 지표가 오르는 걸 사람이 볼 때까지 아무도 모른다.

**LISTEN 커넥션이 끊긴 것은 `DOWN` 이 아니다.** 폴링 안전망이 덮으므로 릴레이는 계속 동작한다. `DOWN` 으로 만들면 멀쩡한 파드가 재시작된다. 상세에 상태를 싣고, 감시는 `relay.listener.connected` 게이지로 한다.

```yaml
management:
  endpoint:
    health:
      show-details: always
      group:
        liveness:  { include: drainer }
        readiness: { include: db, drainer }
```

---

## 9. 관측

### 9.1 지표

Micrometer → Prometheus, `relay.*` 접두사. **`eventId` 를 태그로 쓰지 않는다.**

지표 이름과 태그 값의 조합 하나가 시계열 하나다. `result` 처럼 값이 두 가지뿐인 태그는 시계열도 둘로 끝나지만, `eventId` 는 이벤트마다 다르고 계속 새로 생기므로 발행 건수만큼 시계열이 늘어난다. 수집기가 그 인덱스를 전부 메모리에 들고 있어야 해서 하루 이틀이면 감당하지 못한다. 특정 이벤트를 좇는 것은 지표가 아니라 로그의 일이다 — 식별자는 MDC 로 남긴다.

| 지표 | 타입 | 의미 |
| --- | --- | --- |
| `relay.publish.latency` | Timer | `created_at` → `published_at`. 파이프라인 전체 지연 |
| `relay.publish.total{result}` | Counter | 발행 성공 / 실패 |
| `relay.drain.batch.size` | Summary | 사이클당 선점 건수. `batchSize` 에 계속 붙으면 처리량 부족 |
| `relay.outbox.pending` | Gauge | 적체 |
| `relay.outbox.dead` | Gauge | `DEAD` 중 복구 대상 |
| `relay.outbox.held` | Gauge | `DEAD` 중 정지된 것. 잊힌 정지 감지 |
| `relay.dead.transition.total` | Counter | `DEAD` 전환 횟수. 순환 감지의 핵심 |
| `relay.dead.recovery.total` | Counter | 자동 복구 횟수 |
| `relay.zombie.reclaim.total` | Counter | 좀비 회수 건수 |
| `relay.stale.write.total` | Counter | 소유권 확인에 튕긴 결과 쓰기   미구현 |
| `relay.mark.failure.total` | Counter | 결과 기록 자체가 실패한 횟수   미구현 |
| `relay.listener.connected` | Gauge | LISTEN 커넥션 상태 (0/1) |
| `relay.listener.reconnect.total` | Counter | 재연결 횟수 |

`relay.publish.latency` 는 `created_at` 부터 재므로 `DEAD` 순환을 거친 이벤트가 수십 분으로 찍혀 히스토그램을 오염시킨다. 첫 시도인지로 태그를 나눠 정상 경로의 지연을 따로 본다. **미구현** — 현재는 태그 없는 Timer 하나다.

게이지는 스크레이핑마다 세지 않고 주기 스케줄러가 한 번의 쿼리로 갱신한다. `PENDING` · `PUBLISHING` 집계는 부분 인덱스를 타고, `DEAD` · `held` 집계는 부분 인덱스가 없어 Seq Scan이다.

### 9.2 알림

| 알림 | 조건 | 뜻 |
| --- | --- | --- |
| 즉시 | `relay.stale.write.total > 0` | 소유권 경합이 실재한다   미구현 |
| 즉시 | `relay.mark.failure.total > 0` | 결과 기록이 실패했다   미구현 |
| 즉시 | `relay.dead.transition.total` 이 5분 안에 상승 | 발행이 계속 실패한다 |
| 5분 지속 | `relay.listener.connected == 0` | 알림 경로가 죽었다. 동작은 하지만 느려진다 |
| 10분 지속 | `relay.outbox.pending` 계속 증가 | 처리량이 못 따라간다 |
| 1시간 지속 | `relay.outbox.held > 0` | 정지시켜 놓고 잊었다 |

`relay.dead.transition.total` 이 `relay.publish.total{result=success}` 보다 빠르게 오르면 순환 중이라는 신호다.

### 9.3 로깅

MDC에 `eventId` · `traceId` 를 싣는다. 로그 한 줄이 여러 행을 다룰 수 있으므로(같은 에러 메시지를 공유하는 배치) 쉼표로 이어 붙인다. `trace_id` 는 읽기만 하고 생성·수정하지 않는다.

---

## 10. 어드민

Actuator `@Endpoint(id = "outbox")`. 관리 포트, `127.0.0.1` 바인딩.

| 요청 | 하는 일 |
| --- | --- |
| `GET /actuator/outbox` | 상태별 집계 (`pending` / `publishing` / `dead` / `held`) |
| `GET /actuator/outbox/dead` | `DEAD` 목록. 각 행에 `held` 와 `nextAttemptAt` 포함 |
| `POST /actuator/outbox/{id}` | body `{"action": ...}` |

| action | 수행 | 대상 |
| --- | --- | --- |
| `REPUBLISH` | `PENDING`, 카운트 0, `next_attempt_at = now()` + 신호 | `DEAD` 행 |
| `HOLD` | `next_attempt_at = 'infinity'` | `DEAD` 행 |
| `RELEASE` | `next_attempt_at = now()` | `DEAD` 행 |
| `FORCE_REPUBLISH` | 같음 | `PUBLISHED` 행   미구현 |

전부 기존 행 UPDATE다. 어드민 경로도 "릴레이는 INSERT하지 않는다"를 지킨다.

`REPUBLISH` 는 `AND status = 'DEAD'` 로 대상을 좁힌다. 좁히지 않으면 `PENDING` 행도 잘못 건드릴 수 있고, 그렇게 정지된 행은 `held` 게이지에도 `DEAD` 목록에도 안 잡히면서 영원히 발행되지 않는다 — `held` 게이지가 막으려던 바로 그 실패 모드다.

`FORCE_REPUBLISH` 를 따로 두는 이유는 두 가지다. Kafka 브로커가 재시작하면서 `PUBLISHED` 로 기록된 메시지가 실제로는 유실되는 경우가 있고, 그때 되돌릴 수단이 필요하다. 그리고 워커 쪽이 "이 이벤트가 처리되지 않았다"를 판정해 재발행을 요청하는 경로에도 이게 쓰인다 — 판정은 `indexing_job` 을 아는 쪽이 하고, 릴레이는 재발행 수단만 제공한다.

파괴적인 동작이므로 기존 `REPUBLISH` 와 별도 액션으로 두고, `relay.forced.republish.total` 지표와 감사 로그를 남긴다.

`127.0.0.1` 바인딩만으로는 같은 호스트·파드 안의 다른 프로세스를 막지 못한다. 쓰기 액션에는 최소한의 인증을 붙인다. **미구현** — 현재 인증이 없다.

---

## 11. 설정

```yaml
relay:
  instance-id: ${HOSTNAME:doc-relay-local}
  drain:
    batch-size: 100
  polling:
    interval: 10s
  backoff:
    base: 10s
    max: 5m
    max-attempts: 5
  dead:
    recovery-delay: 10m
    recovery-scan-interval: 5m
  zombie:
    lock-timeout: 5m
    scan-interval: 1m
  shutdown:
    drain-timeout: 30s          # 미구현
  listener:
    enabled: true               # 테스트에서 리스너를 끄기 위한 스위치
    channel: outbox_event
    reconnect-base: 1s
    reconnect-max: 30s
  metrics:
    gauge-refresh-interval: 30s
  kafka:
    topic: doc.events.v1
    partitions: 3

spring:
  datasource:
    hikari:                     # 미구현
      connection-timeout: 5000
      validation-timeout: 3000
      keepalive-time: 30000
      max-lifetime: 600000
  task:
    scheduling:
      pool:
        size: 4

server:
  port: -1
management:
  server:
    port: 9090
    address: 127.0.0.1
  endpoints:
    web:
      exposure:
        include: health, info, prometheus, outbox
```

주기 작업이 넷이고(폴링, 좀비 회수, `DEAD` 복구, 게이지 갱신) 그중 셋이 DB를 만진다. 풀을 작업 수보다 작게 두면 DB가 느려질 때 넷이 모두 밀리고, 최후 방어선인 폴링까지 DB 작업 뒤에 줄을 선다. 최소한 작업 수만큼은 두고, 커넥션 타임아웃으로 대기 자체를 끊는다.

DB 페일오버는 이렇게 흘러야 한다.

```
Primary 다운 → 커넥션이 5초 만에 실패 → 사이클이 예외로 끝남 → 루프는 살아 있음
  → 선점된 행은 PUBLISHING 으로 남음
  → promote 완료 → 다음 신호에 정상 동작
  → 좀비 회수가 남은 행을 되돌림 → 유실 0
```

릴레이는 소비자가 아니라 폴러라서, DB가 없으면 할 일 자체가 없다. 소비를 멈추는 별도 장치가 필요하지 않다. 커넥션 타임아웃이 없으면 스레드가 잠기므로 그것만 막는다.

`demo` 프로파일은 시연용으로 주기를 낮춘다. 기본값으로는 Kafka를 죽였다 살릴 때 관객이 `DEAD` 도달 2분 30초 + 복구 10분을 기다리게 된다.

```yaml
relay:
  polling:      { interval: 3s }
  backoff:      { base: 2s, max: 20s }
  dead:         { recovery-delay: 30s, recovery-scan-interval: 10s }
  zombie:       { lock-timeout: 30s, scan-interval: 5s }
```

예산 관계가 여기서도 성립해야 하므로 프로듀서 타임아웃도 함께 줄인다. **미구현** — 프로듀서 타임아웃 자체가 아직 명시되어 있지 않다.

---

## 12. 테스트

Testcontainers(PostgreSQL 17 + Kafka)로 실제 DB와 실제 브로커를 띄운다. 스키마는 픽스처 SQL로 만든다.

테스트는 `outbox_event` 에 직접 INSERT한다. INSERT 금지는 릴레이 런타임에 대한 제약이지 테스트 준비에 대한 제약이 아니다.

### 12.1 먼저 증명할 전제

| # | 시나리오 | 통과 기준 |
| --- | --- | --- |
| 1 | 한 배치로 잡은 행은 같은 `locked_at` 을 갖는다   미구현 | 10건 선점 후 `locked_at` 이 전부 동일 |

소유권 확인의 배치 UPDATE 전체가 여기 걸려 있다. 이게 아니면 소유권 확인을 행 단위로 다시 설계해야 한다.

### 12.2 소유권과 경합

| # | 시나리오 | 통과 기준 |
| --- | --- | --- |
| 2 | 늦게 온 실패 기록이 `PUBLISHED` 를 되돌리지 못한다   미구현 | 선점 → 회수 → 재선점 → 발행 성공 후, 예전 `locked_at` 으로 실패 기록 시도 → 갱신 0행, 상태는 `PUBLISHED` 유지, `relay.stale.write.total` 증가 |
| 3 | 다른 인스턴스의 결과 쓰기가 튕긴다   미구현 | A로 선점 후 B의 instanceId로 기록 시도 → 갱신 0행 |
| 4 | 중복 발행은 계약이다 | 결과 반영 직전 크래시 시뮬 → 같은 `eventId` 2회 발행을 정상으로 확정 |

### 12.3 예산과 기동

| # | 시나리오 | 통과 기준 |
| --- | --- | --- |
| 5 | 예산이 어긋나면 기동하지 않는다   미구현 | 좀비 타임아웃 10초 + 프로듀서 상한 120초 → 즉시 실패, 메시지에 두 값이 찍힘 |
| 6 | `demo` 프로파일도 예산을 만족한다   미구현 | 같은 검사 통과 |
| 7 | 스키마 검증 | `next_attempt_at` nullable 픽스처로 기동 → 즉시 실패. 인덱스만 없으면 → 기동 + WARN |

### 12.4 실패 분류와 복구

| # | 시나리오 | 통과 기준 |
| --- | --- | --- |
| 8 | 브로커 상한 초과 메시지   미구현 | 1회 만에 `DEAD` + `'infinity'`. 재시도 없음. `held` 증가 |
| 9 | payload가 깨진 행   미구현 | 위와 동일 |
| 10 | Kafka 중지 | 5회 재시도 → `DEAD` → 복구 지연 후 `PUBLISHED` |
| 11 | 정지 스위치 | `HOLD` 후 복구 스캔 반복 → 여전히 `DEAD`. `RELEASE` 후 발행 |
| 12 | 좀비 회수 | `PUBLISHING` + 오래된 `locked_at` → 회수 후 발행, 카운트 증가 |

### 12.5 신호 경로

| # | 시나리오 | 통과 기준 |
| --- | --- | --- |
| 13 | LISTEN 재연결 | `pg_terminate_backend` 로 강제 종료 → 그 사이 INSERT된 이벤트가 재연결 후 발행 |
| 14 | 알림 유실 | 리스너를 끈 채 INSERT → 폴링 주기 내 발행 |
| 15 | 신호 폭주 | 신호 1000개 → 추가 사이클 최대 1회, 남는 행 없음 |
| 16 | 알림 페이로드 미사용 | 페이로드를 조회 키로 쓰지 않음을 확인 |

### 12.6 수명주기

| # | 시나리오 | 통과 기준 |
| --- | --- | --- |
| 17 | 정상 종료는 좀비를 남기지 않는다   미구현 | 드레인 중 컨텍스트 close → `PUBLISHING` 잔여 0건 |
| 18 | 안 끝나면 시간 안에 포기한다   미구현 | 발행을 느리게 → 대기 상한 안에 종료 완료 |
| 19 | 드레인 스레드가 죽으면 `DOWN`   미구현 | 스레드 강제 종료 → 헬스체크 `DOWN`, 상세에 이유 |
| 20 | 리스너가 끊겨도 `UP`   미구현 | 커넥션만 끊음 → `UP` 유지, 이벤트는 폴링으로 계속 발행 |

### 12.7 그 외

| # | 시나리오 | 통과 기준 |
| --- | --- | --- |
| 21 | 컨트롤러 금지 | `@Controller` / `@RestController` 빈 0개 |
| 22 | 문서 단위 순서 | 같은 `document_id` 이벤트 N개가 같은 파티션에 순서대로 |
| 23 | 봉투 조립 | payload 무파싱 통과, `eventType` 무분기 복사, 최상위 필드는 컬럼에서 복원 |
| 24 | 한 행의 조립 실패가 배치를 죽이지 않는다 | 깨진 행 1건 + 정상 9건 → 9건 발행, 1건만 실패 |

### 12.8 무작위 장애 주입

30분 동안 릴레이를 무작위로 죽이고, Kafka를 죽였다 살리고, DB를 재시작하면서 계속 업로드한다.

```sql
-- ① 모든 행이 최종 상태에 도달했는가
SELECT status, count(*) FROM outbox_event GROUP BY status;
-- 좀비 회수 주기보다 오래 기다린 뒤 PUBLISHING 이 남아 있으면 안 된다

-- ② INSERT 건수 == Kafka에 도착한 고유 eventId 건수  → 유실 0
--    중복 도착은 계약상 정상이므로 고유 개수로 센다
```

`relay.stale.write.total` 이 0보다 크면 소유권 확인이 실제로 경합을 튕겨냈다는 뜻이다. **미구현**

---

## 13. 시연

`demo/` 아래 스크립트와 `demo` 프로파일. 각 단계가 "INSERT 건수 == Kafka 도착 고유 건수"를 검증한다.

| # | 주입 | 보여줄 것 |
| --- | --- | --- |
| 1 | 정상 업로드 | INSERT → 1초 내 Kafka 도착 (LISTEN 즉시성) |
| 2 | 발행 도중 `kill -9` | 재시작 → 좀비 회수 → 유실 0 |
| 3 | 정상 종료(SIGTERM) | 하던 배치를 끝내고 내려감 → `PUBLISHING` 잔여 0건   미구현 |
| 4 | Kafka 중지 후 계속 업로드 | 적체 그래프 → 재개 → 배치 회복 |
| 5 | Kafka 장기 중지 | `DEAD` 도달 → 자동 복구 → 발행 |
| 6 | 브로커 상한 초과 메시지 | 1회 만에 `DEAD` + 자동 정지 → 순환하지 않음 → 고친 뒤 `RELEASE`   미구현 |
| 7 | LISTEN 커넥션 강제 종료 | 폴링만으로 계속 동작 → 재연결 후 즉시성 회복 |

2번과 3번을 나란히 보여준다. 강제 종료는 좀비 회수가 6분 걸려 처리하고, 정상 종료는 즉시 끝난다. 둘의 차이가 한 화면에 들어온다.

---

## 14. 구현 순서

| 순서 | 항목 | 선행 | 이유 |
| --- | --- | --- | --- |
| 1 | 빌드 정리 · 픽스처 · 스키마 검증 | — | 잘못된 스키마로 기동하면 즉시 실패 |
| 2 | 설정 프로퍼티 · 백오프 | 1 | 숫자를 코드에서 뺀다 |
| 3 | 선점 · 결과 반영 SQL (소유권 확인 포함)   미구현 | 2 | 소유권 확인을 처음부터 넣는다. 나중에 붙이면 이미 도는 시스템을 건드리게 된다 |
| 4 | 프로듀서 설정 · 예산 검증   미구현 | 2 | 예산이 소유권 확인의 전제다 |
| 5 | 봉투 조립 · 발행 | 4 | |
| 6 | 드레인 사이클 조립 | 3, 5 | 여기서 처음 end-to-end로 돈다 |
| 7 | 신호 합치기 · 폴링 | 6 | |
| 8 | LISTEN 리스너 | 7 | 폴링이 먼저 있어야 리스너 없이도 검증된다 |
| 9 | 좀비 회수 | 6 | |
| 10 | 실패 분류   미구현 | 9 | 잘못 분류하면 멈춰선 안 될 게 멈추므로 회수가 먼저 |
| 11 | `DEAD` 복구 · 정지 스위치 | 10 | |
| 12 | 지표 · 헬스체크   일부 미구현(헬스체크) | 11 | 나머지가 안전한지 확인할 수단 |
| 13 | 종료 처리   미구현 | 12 | 헬스체크가 있어야 검증된다 |
| 14 | 어드민 엔드포인트 | 12 | |
| 15 | 장애 주입 시연 | 전부 | |

소유권 확인을 3번에 두는 이유는, 이것이 나중에 덧붙이는 방어가 아니라 선점 SQL의 `RETURNING` 과 결과 반영 SQL의 `WHERE` 를 함께 정하는 설계이기 때문이다. 나중에 붙이면 두 SQL을 동시에 바꿔야 한다.

예산을 소유권 확인보다 앞이 아니라 옆에 두는 이유는, 소유권 확인이 "회수가 내 배치를 뺏기 전에 내가 끝낸다"를 전제하기 때문이다. 상한이 없으면 그 전제가 성립하지 않아 정상 동작이 튕기기 시작한다.

---

## 15. 외부 의존성

| 대상 | 내용 | 막는 것 |
| --- | --- | --- |
| 파트너 스키마 | `next_attempt_at` 리네이밍 + NOT NULL, `DEAD` CHECK, `idx_outbox_pending` 재정의 | dev DB 연동. 로컬은 픽스처로 진행 가능 |
| 파트너 스키마 | `PUBLISHED` 행 보존 정책 — N일 후 삭제 또는 `created_at` 기준 파티셔닝 | 없어도 동작하지만 테이블이 무한히 자라고 `DEAD` 집계가 선형으로 비싸진다 |
| 파트너 스키마 | `DEAD` 조회용 부분 인덱스 | 없어도 동작. 시연에서 실행 계획 실측 후 판단 |
| 그룹2 | 봉투에서 `payload` 블록 제거, 식별자만 평평하게 | 봉투 형태 확정. 확정되면 `EnvelopeAssembler` 만 고친다 |
| 인프라 | liveness probe를 `/actuator/health/liveness` 로 | 헬스체크가 여기 물린다   미구현 |
| 인프라 | graceful shutdown 유예를 종료 처리의 대기 상한보다 길게 | 짧으면 마무리하다 잘린다   미구현 |
| 인프라 | 관리 포트(9090)가 파드 밖으로 나가지 않게 | |

릴레이가 `PUBLISHED` 행을 지우지 않는 것은 경계 설정 때문이다. 보존 정책이 없으면 테이블이 무한히 자라고, 부분 인덱스가 없는 `DEAD` · `held` 집계가 그만큼 비싸진다. 인덱스를 붙여도 증식 자체는 멈추지 않으므로 두 요청은 별개다.

---

## 부록. 설계 원칙

1. **락을 쥔 채 네트워크로 나가지 않는다.** 느린 쪽이 빠른 쪽을 인질로 잡지 못하게 한다.
2. **신호는 깨우기만 한다. 무엇을 보낼지는 항상 DB에 다시 묻는다.** 그래서 알림 경로가 통째로 죽어도 데이터를 잃지 않는다. 느려질 뿐이다.
3. **종착 상태를 두지 않는다. 대신 사람이 멈출 수 있게 한다.** 그 대가는 새 상태를 만들지 말고 기존 정지 스위치로 갚는다.
4. **소유권은 사이클 도중에 넘어갈 수 있다고 가정한다.** 회수 장치가 있는 시스템에서 결과 쓰기는 항상 소유권을 확인해야 한다.
5. **재시도 예산은 회수 타임아웃 안에 들어가야 한다.** 넘기는 순간 회수가 정상 동작을 가로챈다.
6. **릴레이는 `outbox_event` 에 INSERT도 DELETE도 하지 않는다.** 모든 쓰기가 UPDATE다. 어드민 경로도 예외가 아니다.
7. **다른 팀의 테이블을 읽지 않는다.** 남의 테이블의 의미를 알아야 하는 판정은 그 테이블을 소유한 쪽이 한다.
8. **모르면 재시도하는 쪽을 고른다.** 잘못 재시도하면 낭비지만, 잘못 멈추면 데이터가 나가지 않는다.
9. **조용한 실패보다 시끄러운 실패가 낫다.** 스키마가 어긋나면 기동을 막고, 예산이 깨져도 기동을 막는다.
10. **감지할 수 없으면 대비한 게 아니다.** 헬스체크에 연결되지 않은 방어 로직은 없는 것과 같다.
11. **exactly-once를 추구하지 않는다.** at-least-once를 계약으로 못 박고, 중복은 받는 쪽이 흡수한다.
